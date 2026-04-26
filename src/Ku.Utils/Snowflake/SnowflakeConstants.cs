namespace Ku.Utils.Snowflake;

/// <summary>
/// Snowflake ID 的位布局常量。
/// </summary>
public static class SnowflakeConstants
{
    /// <summary>
    /// workerId 占用的位数。
    /// </summary>
    public const int WorkerIdBits = 10;

    /// <summary>
    /// 毫秒内序列号占用的位数。
    /// </summary>
    public const int SequenceBits = 12;

    /// <summary>
    /// 最大 workerId。
    /// </summary>
    public const long MaxWorkerId = -1L ^ (-1L << WorkerIdBits);

    /// <summary>
    /// 最大毫秒内序列号。
    /// </summary>
    public const long MaxSequence = -1L ^ (-1L << SequenceBits);

    /// <summary>
    /// workerId 左移位数。
    /// </summary>
    public const int WorkerIdShift = SequenceBits;

    /// <summary>
    /// 时间戳左移位数。
    /// </summary>
    public const int TimestampShift = SequenceBits + WorkerIdBits;

    /// <summary>
    /// 默认纪元时间，2020-01-01T00:00:00Z。
    /// </summary>
    public const long DefaultEpoch = 1_577_836_800_000;
}
