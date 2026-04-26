using Ku.Utils.Database.PostgreSql;

namespace Ku.Utils.Snowflake;

/// <summary>
/// 分布式 Snowflake ID 生成器配置。
/// </summary>
public sealed record DistributedSnowflakeOptions
{
    /// <summary>
    /// PostgreSQL 连接字符串。
    /// </summary>
    public required string ConnectionString { get; init; }

    /// <summary>
    /// worker 租约表所在 schema。
    /// </summary>
    public string Schema { get; init; } = "infra";

    /// <summary>
    /// worker 租约表名。
    /// </summary>
    public string TableName { get; init; } = "snowflake_workers";

    /// <summary>
    /// Snowflake 纪元时间戳，单位为毫秒。
    /// </summary>
    public long Epoch { get; init; } = SnowflakeConstants.DefaultEpoch;

    /// <summary>
    /// worker 租约心跳间隔。
    /// </summary>
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// worker 多久未心跳后可被其他实例回收。
    /// </summary>
    public TimeSpan WorkerTimeout { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// 本实例多久未成功心跳后拒绝继续生成 ID。
    /// </summary>
    public TimeSpan SafetyThreshold { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// 启动时允许等待系统时钟追平的最大时间。
    /// </summary>
    public TimeSpan MaximumStartupClockDrift { get; init; } = TimeSpan.FromSeconds(5);

    internal PostgreSqlConnectionOptions ToPostgreSqlConnectionOptions()
    {
        return new PostgreSqlConnectionOptions
        {
            ConnectionString = ConnectionString
        };
    }
}
