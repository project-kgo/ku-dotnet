using System.Collections.Concurrent;
using Npgsql;

namespace Ku.Utils.Database.PostgreSql;

/// <summary>
/// 按连接配置复用 PostgreSQL 数据源和连接池。
/// </summary>
public static class PostgreSqlDataSourceFactory
{
    private static readonly ConcurrentDictionary<string, Lazy<NpgsqlDataSource>> DataSources = new(StringComparer.Ordinal);

    /// <summary>
    /// 获取或创建共享的 <see cref="NpgsqlDataSource"/>。
    /// </summary>
    /// <param name="options">PostgreSQL 连接配置。</param>
    /// <returns>相同配置对应的共享数据源。</returns>
    public static NpgsqlDataSource GetOrCreate(PostgreSqlConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var cacheKey = BuildConnectionString(options);

        return DataSources.GetOrAdd(
            cacheKey,
            static key => new Lazy<NpgsqlDataSource>(
                () => NpgsqlDataSource.Create(key),
                LazyThreadSafetyMode.ExecutionAndPublication)).Value;
    }

    internal static string BuildConnectionString(PostgreSqlConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("PostgreSQL 连接字符串不能为空。", nameof(options));
        }

        var builder = new NpgsqlConnectionStringBuilder(options.ConnectionString);

        if (options.Pooling is { } pooling)
        {
            builder.Pooling = pooling;
        }

        if (options.MinimumPoolSize is { } minimumPoolSize)
        {
            builder.MinPoolSize = ValidateNonNegative(minimumPoolSize, nameof(options.MinimumPoolSize));
        }

        if (options.MaximumPoolSize is { } maximumPoolSize)
        {
            builder.MaxPoolSize = ValidatePositive(maximumPoolSize, nameof(options.MaximumPoolSize));
        }

        if (options.Timeout is { } timeout)
        {
            builder.Timeout = ValidateSeconds(timeout, nameof(options.Timeout));
        }

        if (options.CommandTimeout is { } commandTimeout)
        {
            builder.CommandTimeout = ValidateSeconds(commandTimeout, nameof(options.CommandTimeout));
        }

        if (options.ConnectionIdleLifetime is { } connectionIdleLifetime)
        {
            builder.ConnectionIdleLifetime = ValidateSeconds(connectionIdleLifetime, nameof(options.ConnectionIdleLifetime));
        }

        if (options.ConnectionPruningInterval is { } connectionPruningInterval)
        {
            builder.ConnectionPruningInterval = ValidateSeconds(connectionPruningInterval, nameof(options.ConnectionPruningInterval));
        }

        if (options.ConnectionLifetime is { } connectionLifetime)
        {
            builder.ConnectionLifetime = ValidateSeconds(connectionLifetime, nameof(options.ConnectionLifetime));
        }

        if (builder.MinPoolSize > builder.MaxPoolSize)
        {
            throw new ArgumentException("连接池最小连接数不能大于最大连接数。", nameof(options));
        }

        return BuildStableConnectionString(builder);
    }

    private static string BuildStableConnectionString(NpgsqlConnectionStringBuilder builder)
    {
        var stableBuilder = new NpgsqlConnectionStringBuilder();
        var keys = builder.Keys
            .Cast<string>()
            .Order(StringComparer.OrdinalIgnoreCase);

        foreach (var key in keys)
        {
            stableBuilder[key] = builder[key];
        }

        return stableBuilder.ConnectionString;
    }

    private static int ValidateNonNegative(int value, string argumentName)
    {
        return value < 0
            ? throw new ArgumentOutOfRangeException(argumentName, "参数不能小于 0。")
            : value;
    }

    private static int ValidatePositive(int value, string argumentName)
    {
        return value <= 0
            ? throw new ArgumentOutOfRangeException(argumentName, "参数必须大于 0。")
            : value;
    }

    private static int ValidateSeconds(TimeSpan value, string argumentName)
    {
        if (value < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(argumentName, "时间参数不能小于 0。");
        }

        return checked((int)Math.Ceiling(value.TotalSeconds));
    }
}
