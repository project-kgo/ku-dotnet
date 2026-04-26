namespace Ku.Utils.Database.Redis;

/// <summary>
/// Redis 连接配置。
/// </summary>
public sealed record RedisConnectionOptions
{
    /// <summary>
    /// Redis 连接字符串。
    /// </summary>
    public required string ConnectionString { get; init; }

    /// <summary>
    /// 是否启用共享连接复用。
    /// </summary>
    public bool? Pooling { get; init; }

    /// <summary>
    /// 建立连接的超时时间。
    /// </summary>
    public TimeSpan? ConnectTimeout { get; init; }

    /// <summary>
    /// 同步命令执行超时时间。
    /// </summary>
    public TimeSpan? SyncTimeout { get; init; }

    /// <summary>
    /// 异步命令执行超时时间。
    /// </summary>
    public TimeSpan? AsyncTimeout { get; init; }

    /// <summary>
    /// TCP 保活检测间隔。
    /// </summary>
    public TimeSpan? KeepAlive { get; init; }

    /// <summary>
    /// 初始连接重试次数。
    /// </summary>
    public int? ConnectRetry { get; init; }

    /// <summary>
    /// 初始连接失败时是否中止。
    /// </summary>
    public bool? AbortOnConnectFail { get; init; }

    /// <summary>
    /// 默认 Redis 数据库编号。
    /// </summary>
    public int? DefaultDatabase { get; init; }

    /// <summary>
    /// Redis 客户端名称。
    /// </summary>
    public string? ClientName { get; init; }
}
