using System.Collections.Concurrent;
using StackExchange.Redis;

namespace Ku.Utils.Database.Redis;

/// <summary>
/// 按连接配置复用 Redis 共享连接。
/// </summary>
public static class RedisConnectionFactory
{
    private static readonly ConcurrentDictionary<string, IConnectionMultiplexer> Connections = new(StringComparer.Ordinal);
    private static readonly Lock _syncRoot = new();

    internal static Func<string, IConnectionMultiplexer> ConnectionFactory { get; set; } = static connectionString => ConnectionMultiplexer.Connect(connectionString);

    /// <summary>
    /// 获取或创建共享的 <see cref="IConnectionMultiplexer"/>。
    /// </summary>
    /// <param name="options">Redis 连接配置。</param>
    /// <returns>相同配置对应的共享连接。</returns>
    public static IConnectionMultiplexer GetOrCreate(RedisConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var configuration = BuildConfiguration(options);

        if (Connections.TryGetValue(configuration.CacheKey, out var connection))
        {
            return connection;
        }

        lock (_syncRoot)
        {
            if (Connections.TryGetValue(configuration.CacheKey, out connection))
            {
                return connection;
            }

            connection = ConnectionFactory(configuration.ConnectionString);
            Connections[configuration.CacheKey] = connection;

            return connection;
        }
    }

    internal static string BuildConfigurationString(RedisConnectionOptions options)
    {
        return BuildConfiguration(options).CacheKey;
    }

    internal static void ResetForTests()
    {
        Connections.Clear();
        ConnectionFactory = static connectionString => ConnectionMultiplexer.Connect(connectionString);
    }

    private static RedisConfiguration BuildConfiguration(RedisConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("Redis 连接字符串不能为空。", nameof(options));
        }

        var (connectionString, pooling) = ExtractPooling(options.ConnectionString);
        var configuration = ConfigurationOptions.Parse(connectionString);

        if (options.Pooling is { } typedPooling)
        {
            pooling = typedPooling;
        }

        if (options.ConnectTimeout is { } connectTimeout)
        {
            configuration.ConnectTimeout = ValidateMilliseconds(connectTimeout, nameof(options.ConnectTimeout));
        }

        if (options.SyncTimeout is { } syncTimeout)
        {
            configuration.SyncTimeout = ValidateMilliseconds(syncTimeout, nameof(options.SyncTimeout));
        }

        if (options.AsyncTimeout is { } asyncTimeout)
        {
            configuration.AsyncTimeout = ValidateMilliseconds(asyncTimeout, nameof(options.AsyncTimeout));
        }

        if (options.KeepAlive is { } keepAlive)
        {
            configuration.KeepAlive = ValidateSeconds(keepAlive, nameof(options.KeepAlive));
        }

        if (options.ConnectRetry is { } connectRetry)
        {
            configuration.ConnectRetry = ValidateNonNegative(connectRetry, nameof(options.ConnectRetry));
        }

        if (options.AbortOnConnectFail is { } abortOnConnectFail)
        {
            configuration.AbortOnConnectFail = abortOnConnectFail;
        }

        if (options.DefaultDatabase is { } defaultDatabase)
        {
            configuration.DefaultDatabase = ValidateNonNegative(defaultDatabase, nameof(options.DefaultDatabase));
        }

        if (options.ClientName is { } clientName)
        {
            configuration.ClientName = clientName;
        }

        var redisConnectionString = configuration.ToString();
        var cacheKey = pooling is { } poolingValue
            ? $"{redisConnectionString},pooling={poolingValue.ToString().ToLowerInvariant()}"
            : redisConnectionString;

        return new RedisConfiguration(cacheKey, redisConnectionString);
    }

    private static (string ConnectionString, bool? Pooling) ExtractPooling(string connectionString)
    {
        bool? pooling = null;
        var segments = connectionString.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        var supportedSegments = new List<string>(segments.Length);

        foreach (var segment in segments)
        {
            var separatorIndex = segment.IndexOf('=');
            if (separatorIndex <= 0 || !segment[..separatorIndex].Equals("pooling", StringComparison.OrdinalIgnoreCase))
            {
                supportedSegments.Add(segment);
                continue;
            }

            if (!bool.TryParse(segment[(separatorIndex + 1)..], out var parsedPooling))
            {
                throw new ArgumentException("Redis 连接池配置必须是 true 或 false。", nameof(connectionString));
            }

            pooling = parsedPooling;
        }

        if (supportedSegments.Count == 0)
        {
            throw new ArgumentException("Redis 连接字符串不能为空。", nameof(connectionString));
        }

        return (string.Join(',', supportedSegments), pooling);
    }

    private static int ValidateNonNegative(int value, string argumentName)
    {
        return value < 0
            ? throw new ArgumentOutOfRangeException(argumentName, "参数不能小于 0。")
            : value;
    }

    private static int ValidateMilliseconds(TimeSpan value, string argumentName)
    {
        if (value < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(argumentName, "时间参数不能小于 0。");
        }

        return checked((int)Math.Ceiling(value.TotalMilliseconds));
    }

    private static int ValidateSeconds(TimeSpan value, string argumentName)
    {
        if (value < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(argumentName, "时间参数不能小于 0。");
        }

        return checked((int)Math.Ceiling(value.TotalSeconds));
    }

    private sealed record RedisConfiguration(string CacheKey, string ConnectionString);
}
