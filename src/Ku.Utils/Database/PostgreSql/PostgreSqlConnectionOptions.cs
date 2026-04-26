namespace Ku.Utils.Database.PostgreSql;

/// <summary>
/// PostgreSQL 连接配置。
/// </summary>
public sealed record PostgreSqlConnectionOptions
{
    /// <summary>
    /// PostgreSQL 连接字符串。
    /// </summary>
    public required string ConnectionString { get; init; }

    /// <summary>
    /// 是否启用连接池。
    /// </summary>
    public bool? Pooling { get; init; }

    /// <summary>
    /// 连接池最小连接数。
    /// </summary>
    public int? MinimumPoolSize { get; init; }

    /// <summary>
    /// 连接池最大连接数。
    /// </summary>
    public int? MaximumPoolSize { get; init; }

    /// <summary>
    /// 建立连接的超时时间。
    /// </summary>
    public TimeSpan? Timeout { get; init; }

    /// <summary>
    /// 命令执行超时时间。
    /// </summary>
    public TimeSpan? CommandTimeout { get; init; }

    /// <summary>
    /// 空闲连接在连接池中保留的最长时间。
    /// </summary>
    public TimeSpan? ConnectionIdleLifetime { get; init; }

    /// <summary>
    /// 连接池清理空闲连接的扫描间隔。
    /// </summary>
    public TimeSpan? ConnectionPruningInterval { get; init; }

    /// <summary>
    /// 物理连接可被复用的最长时间。
    /// </summary>
    public TimeSpan? ConnectionLifetime { get; init; }
}
