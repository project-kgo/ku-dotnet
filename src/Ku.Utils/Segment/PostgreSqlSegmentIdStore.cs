using Ku.Utils.Database.PostgreSql;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;

namespace Ku.Utils.Segment;

internal sealed class PostgreSqlSegmentIdStore(NpgsqlDataSource dataSource, string schema, string tableName) : ISegmentIdStore
{
    private readonly string _schema = PostgreSqlIdentifier.Quote(schema, nameof(schema));
    private readonly string _tableName = PostgreSqlIdentifier.Quote(tableName, nameof(tableName));

    public async Task EnsureTableAndRecordAsync(int bizTag, long startId, int step, CancellationToken cancellationToken)
    {
        await using var context = PostgreSqlDbContextFactory.Create(dataSource);

        var createTableSql = $"""
            CREATE TABLE IF NOT EXISTS {FullTableName} (
                biz_tag INT PRIMARY KEY,
                max_id BIGINT NOT NULL DEFAULT 0,
                step INT NOT NULL DEFAULT 1000,
                description VARCHAR(256),
                update_time TIMESTAMP WITH TIME ZONE NOT NULL
            );
            """;

        await context.Database.ExecuteSqlRawAsync(createTableSql, cancellationToken);

        var initRecordSql = $"""
            INSERT INTO {FullTableName} (biz_tag, max_id, step, update_time)
            VALUES (@biz_tag, @max_id, @step, NOW())
            ON CONFLICT (biz_tag) DO UPDATE
            SET step = @step,
                update_time = NOW()
            """;

        await context.Database.ExecuteSqlRawAsync(
            initRecordSql,
            [
                CreateIntegerParameter("biz_tag", bizTag),
                CreateBigIntParameter("max_id", startId),
                CreateIntegerParameter("step", step)
            ],
            cancellationToken);
    }

    public async Task<Segment> FetchSegmentAsync(int bizTag, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();

        command.CommandText = $"""
            UPDATE {FullTableName}
            SET max_id = max_id + step,
                update_time = NOW()
            WHERE biz_tag = @biz_tag
            RETURNING max_id, step
            """;
        command.Parameters.Add(CreateIntegerParameter("biz_tag", bizTag));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"biz_tag {bizTag} 不存在。");
        }

        var maxId = reader.GetInt64(0);
        var step = reader.GetInt32(1);
        var start = maxId - step + 1L;

        return new Segment(start, maxId);
    }

    private string FullTableName => $"{_schema}.{_tableName}";

    private static NpgsqlParameter CreateIntegerParameter(string name, int value)
    {
        return new NpgsqlParameter(name, NpgsqlDbType.Integer)
        {
            Value = value
        };
    }

    private static NpgsqlParameter CreateBigIntParameter(string name, long value)
    {
        return new NpgsqlParameter(name, NpgsqlDbType.Bigint)
        {
            Value = value
        };
    }
}
