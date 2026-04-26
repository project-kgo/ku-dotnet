using System.Globalization;
using Ku.Utils.Database.PostgreSql;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using NpgsqlTypes;

namespace Ku.Utils.Snowflake;

internal sealed class PostgreSqlSnowflakeWorkerStore(NpgsqlDataSource dataSource, string schema, string tableName, TimeProvider timeProvider)
{
    private readonly string _schema = PostgreSqlIdentifier.Quote(schema, nameof(schema));
    private readonly string _tableName = PostgreSqlIdentifier.Quote(tableName, nameof(tableName));

    public async Task EnsureTableAsync(CancellationToken cancellationToken)
    {
        await using var context = PostgreSqlDbContextFactory.Create(dataSource);

        var sql = $"""
            CREATE TABLE IF NOT EXISTS {FullTableName} (
                worker_id INTEGER PRIMARY KEY,
                last_timestamp BIGINT NOT NULL,
                ip_address TEXT NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL
            );
            """;

        await context.Database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    public async Task<SnowflakeWorkerLease> AllocateWorkerAsync(TimeSpan workerTimeout, CancellationToken cancellationToken)
    {
        await using var context = PostgreSqlDbContextFactory.Create(dataSource);
        await context.Database.OpenConnectionAsync(cancellationToken);
        await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);

        var connection = (NpgsqlConnection)context.Database.GetDbConnection();
        var dbTransaction = (NpgsqlTransaction)transaction.GetDbTransaction();
        var now = timeProvider.GetUtcNow();
        var ipAddress = GetLocalIpAddress();
        var globalMaxTimestamp = await GetGlobalMaxTimestampAsync(connection, dbTransaction, cancellationToken);

        var expiredLease = await TryGetExpiredWorkerAsync(connection, dbTransaction, now - workerTimeout, cancellationToken);
        if (expiredLease is not null)
        {
            await UpdateWorkerAsync(connection, dbTransaction, expiredLease.Value.WorkerId, now, ipAddress, cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            return new SnowflakeWorkerLease(
                expiredLease.Value.WorkerId,
                Math.Max(expiredLease.Value.LastTimestamp, globalMaxTimestamp));
        }

        var nextWorkerId = await GetNextWorkerIdAsync(connection, dbTransaction, cancellationToken);
        if (nextWorkerId > SnowflakeConstants.MaxWorkerId)
        {
            throw new InvalidOperationException("没有可用的 Snowflake workerId。");
        }

        await InsertWorkerAsync(connection, dbTransaction, nextWorkerId, now, ipAddress, cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        return new SnowflakeWorkerLease(nextWorkerId, globalMaxTimestamp);
    }

    public async Task UpdateHeartbeatAsync(long workerId, CancellationToken cancellationToken)
    {
        await using var context = PostgreSqlDbContextFactory.Create(dataSource);
        var now = timeProvider.GetUtcNow();

        var sql = $"""
            UPDATE {FullTableName}
            SET last_timestamp = @last_timestamp,
                updated_at = @updated_at
            WHERE worker_id = @worker_id
            """;

        var rows = await context.Database.ExecuteSqlRawAsync(
            sql,
            [
                CreateBigIntParameter("last_timestamp", now.ToUnixTimeMilliseconds()),
                CreateTimestampParameter("updated_at", now),
                CreateIntegerParameter("worker_id", workerId)
            ],
            cancellationToken);

        if (rows == 0)
        {
            throw new InvalidOperationException($"Snowflake workerId {workerId} 的租约不存在。");
        }
    }

    private string FullTableName => $"{_schema}.{_tableName}";

    private async Task<SnowflakeWorkerLease?> TryGetExpiredWorkerAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DateTimeOffset expiredBefore,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT worker_id, last_timestamp
            FROM {FullTableName}
            WHERE updated_at < @expired_before
            ORDER BY updated_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """;
        command.Parameters.Add(CreateTimestampParameter("expired_before", expiredBefore));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new SnowflakeWorkerLease(reader.GetInt32(0), reader.GetInt64(1));
    }

    private async Task<long> GetGlobalMaxTimestampAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT MAX(last_timestamp) FROM {FullTableName}";

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null or DBNull ? 0L : Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    private async Task<long> GetNextWorkerIdAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT MAX(worker_id) FROM {FullTableName}";

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null or DBNull ? 0L : Convert.ToInt64(result, CultureInfo.InvariantCulture) + 1L;
    }

    private async Task UpdateWorkerAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long workerId,
        DateTimeOffset now,
        string ipAddress,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            UPDATE {FullTableName}
            SET last_timestamp = @last_timestamp,
                ip_address = @ip_address,
                updated_at = @updated_at
            WHERE worker_id = @worker_id
            """;
        command.Parameters.Add(CreateBigIntParameter("last_timestamp", now.ToUnixTimeMilliseconds()));
        command.Parameters.Add(CreateTextParameter("ip_address", ipAddress));
        command.Parameters.Add(CreateTimestampParameter("updated_at", now));
        command.Parameters.Add(CreateIntegerParameter("worker_id", workerId));

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task InsertWorkerAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long workerId,
        DateTimeOffset now,
        string ipAddress,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            INSERT INTO {FullTableName} (worker_id, last_timestamp, ip_address, updated_at)
            VALUES (@worker_id, @last_timestamp, @ip_address, @updated_at)
            """;
        command.Parameters.Add(CreateIntegerParameter("worker_id", workerId));
        command.Parameters.Add(CreateBigIntParameter("last_timestamp", now.ToUnixTimeMilliseconds()));
        command.Parameters.Add(CreateTextParameter("ip_address", ipAddress));
        command.Parameters.Add(CreateTimestampParameter("updated_at", now));

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static NpgsqlParameter CreateIntegerParameter(string name, long value)
    {
        return new NpgsqlParameter(name, NpgsqlDbType.Integer)
        {
            Value = checked((int)value)
        };
    }

    private static NpgsqlParameter CreateBigIntParameter(string name, long value)
    {
        return new NpgsqlParameter(name, NpgsqlDbType.Bigint)
        {
            Value = value
        };
    }

    private static NpgsqlParameter CreateTextParameter(string name, string value)
    {
        return new NpgsqlParameter(name, NpgsqlDbType.Text)
        {
            Value = value
        };
    }

    private static NpgsqlParameter CreateTimestampParameter(string name, DateTimeOffset value)
    {
        return new NpgsqlParameter(name, NpgsqlDbType.TimestampTz)
        {
            Value = value
        };
    }

    private static string GetLocalIpAddress()
    {
        try
        {
            foreach (var address in System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces()
                .SelectMany(networkInterface => networkInterface.GetIPProperties().UnicastAddresses)
                .Select(addressInformation => addressInformation.Address))
            {
                if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork && !System.Net.IPAddress.IsLoopback(address))
                {
                    return address.ToString();
                }
            }
        }
        catch (System.Net.NetworkInformation.NetworkInformationException)
        {
        }

        return "unknown";
    }
}
