using Ku.Utils.Database.PostgreSql;

namespace Ku.Utils.Segment;

/// <summary>
/// Segment ID 生成器配置。
/// </summary>
public sealed record SegmentIdGeneratorOptions
{
    /// <summary>
    /// PostgreSQL 连接字符串。
    /// </summary>
    public required string ConnectionString { get; init; }

    /// <summary>
    /// 号段表所在 schema。
    /// </summary>
    public string Schema { get; init; } = "infra";

    /// <summary>
    /// 号段表名。
    /// </summary>
    public string TableName { get; init; } = "id_generator";

    /// <summary>
    /// 自动初始化业务标识时使用的起始 ID。
    /// </summary>
    public long DefaultStartId { get; init; }

    /// <summary>
    /// 自动初始化业务标识时使用的号段步长。
    /// </summary>
    public int DefaultStep { get; init; } = 1000;

    /// <summary>
    /// 当前号段使用比例达到该阈值后异步预加载下一号段。
    /// </summary>
    public double PreloadRatio { get; init; } = 0.2D;

    /// <summary>
    /// 异步预加载下一号段的超时时间。
    /// </summary>
    public TimeSpan AsyncLoadTimeout { get; init; } = TimeSpan.FromSeconds(5);

    internal PostgreSqlConnectionOptions ToPostgreSqlConnectionOptions()
    {
        return new PostgreSqlConnectionOptions
        {
            ConnectionString = ConnectionString
        };
    }
}
