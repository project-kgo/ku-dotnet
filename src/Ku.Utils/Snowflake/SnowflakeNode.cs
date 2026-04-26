namespace Ku.Utils.Snowflake;

/// <summary>
/// 本地线程安全的 Snowflake ID 生成器。
/// </summary>
public sealed class SnowflakeNode
{
    private readonly Lock _lock = new();
    private readonly TimeProvider _timeProvider;
    private readonly long _workerId;
    private readonly long _epoch;
    private long _timestamp;
    private long _sequence;

    /// <summary>
    /// 创建 Snowflake 节点。
    /// </summary>
    /// <param name="workerId">workerId，取值范围为 0 到 1023。</param>
    /// <param name="epoch">纪元时间戳，单位为毫秒；传入 0 时使用默认纪元。</param>
    /// <param name="timeProvider">时间提供器，未传入时使用系统时间。</param>
    public SnowflakeNode(long workerId, long epoch = SnowflakeConstants.DefaultEpoch, TimeProvider? timeProvider = null)
    {
        if (workerId is < 0 or > SnowflakeConstants.MaxWorkerId)
        {
            throw new ArgumentOutOfRangeException(nameof(workerId), "workerId 必须在 0 到 1023 之间。");
        }

        _workerId = workerId;
        _epoch = epoch == 0 ? SnowflakeConstants.DefaultEpoch : epoch;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    /// 当前节点的 workerId。
    /// </summary>
    public long WorkerId => _workerId;

    /// <summary>
    /// 当前节点使用的纪元时间戳，单位为毫秒。
    /// </summary>
    public long Epoch => _epoch;

    /// <summary>
    /// 生成一个 Snowflake ID。
    /// </summary>
    /// <returns>64 位 Snowflake ID。</returns>
    public long Generate()
    {
        lock (_lock)
        {
            var current = GetCurrentMilliseconds();

            if (current < _timestamp)
            {
                current = WaitUntilAfter(_timestamp);
            }

            if (_timestamp == current)
            {
                _sequence = (_sequence + 1) & SnowflakeConstants.MaxSequence;

                if (_sequence == 0)
                {
                    current = WaitUntilAfter(_timestamp);
                }
            }
            else
            {
                _sequence = 0;
            }

            _timestamp = current;

            return ((current - _epoch) << SnowflakeConstants.TimestampShift)
                | (_workerId << SnowflakeConstants.WorkerIdShift)
                | _sequence;
        }
    }

    /// <summary>
    /// 从 Snowflake ID 中解析 Unix 毫秒时间戳。
    /// </summary>
    /// <param name="id">Snowflake ID。</param>
    /// <returns>Unix 毫秒时间戳。</returns>
    public long GetTimeFromId(long id)
    {
        return (id >> SnowflakeConstants.TimestampShift) + _epoch;
    }

    /// <summary>
    /// 从 Snowflake ID 中解析 UTC 时间。
    /// </summary>
    /// <param name="id">Snowflake ID。</param>
    /// <returns>UTC 时间。</returns>
    public DateTimeOffset GetDateTimeOffset(long id)
    {
        return DateTimeOffset.FromUnixTimeMilliseconds(GetTimeFromId(id));
    }

    private long WaitUntilAfter(long timestamp)
    {
        var current = GetCurrentMilliseconds();

        while (current <= timestamp)
        {
            Thread.Yield();
            current = GetCurrentMilliseconds();
        }

        return current;
    }

    private long GetCurrentMilliseconds()
    {
        return _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
    }
}
