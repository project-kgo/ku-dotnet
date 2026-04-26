using Ku.Utils.Snowflake;

namespace Ku.Utils.Tests;

public sealed class SnowflakeNodeTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(SnowflakeConstants.MaxWorkerId + 1)]
    public void Constructor_WhenWorkerIdIsOutOfRange_Throws(long workerId)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SnowflakeNode(workerId));
    }

    [Fact]
    public void Constructor_WhenEpochIsZero_UsesDefaultEpoch()
    {
        var node = new SnowflakeNode(1, 0);

        Assert.Equal(SnowflakeConstants.DefaultEpoch, node.Epoch);
    }

    [Fact]
    public void Generate_UsesExpectedBitLayout()
    {
        var now = DateTimeOffset.FromUnixTimeMilliseconds(SnowflakeConstants.DefaultEpoch + 1234);
        var node = new SnowflakeNode(17, timeProvider: new FixedTimeProvider(now));

        var id = node.Generate();

        Assert.Equal(now.ToUnixTimeMilliseconds(), node.GetTimeFromId(id));
        Assert.Equal(now, node.GetDateTimeOffset(id));
        Assert.Equal(17, (id >> SnowflakeConstants.WorkerIdShift) & SnowflakeConstants.MaxWorkerId);
        Assert.Equal(0, id & SnowflakeConstants.MaxSequence);
    }

    [Fact]
    public void Generate_WhenCalledWithinSameMillisecond_IncrementsSequence()
    {
        var now = DateTimeOffset.FromUnixTimeMilliseconds(SnowflakeConstants.DefaultEpoch + 2000);
        var node = new SnowflakeNode(3, timeProvider: new FixedTimeProvider(now));

        var first = node.Generate();
        var second = node.Generate();
        var third = node.Generate();

        Assert.Equal(0, first & SnowflakeConstants.MaxSequence);
        Assert.Equal(1, second & SnowflakeConstants.MaxSequence);
        Assert.Equal(2, third & SnowflakeConstants.MaxSequence);
        Assert.True(first < second);
        Assert.True(second < third);
    }

    [Fact]
    public void Generate_WhenSequenceOverflows_WaitsForNextMillisecond()
    {
        var baseTime = DateTimeOffset.FromUnixTimeMilliseconds(SnowflakeConstants.DefaultEpoch + 3000);
        var timeProvider = new SequenceOverflowTimeProvider(baseTime);
        var node = new SnowflakeNode(9, timeProvider: timeProvider);

        var ids = Enumerable.Range(0, (int)SnowflakeConstants.MaxSequence + 2)
            .Select(_ => node.Generate())
            .ToArray();

        var lastId = ids[^1];

        Assert.Equal(baseTime.AddMilliseconds(1).ToUnixTimeMilliseconds(), node.GetTimeFromId(lastId));
        Assert.Equal(0, lastId & SnowflakeConstants.MaxSequence);
        Assert.Equal(ids.Length + 1, timeProvider.CallCount);
    }

    [Fact]
    public void Generate_WhenClockMovesBackwards_WaitsUntilClockCatchesUp()
    {
        var baseTime = DateTimeOffset.FromUnixTimeMilliseconds(SnowflakeConstants.DefaultEpoch + 4000);
        var timeProvider = new QueueTimeProvider(
            baseTime,
            baseTime.AddMilliseconds(-1),
            baseTime.AddMilliseconds(1));
        var node = new SnowflakeNode(5, timeProvider: timeProvider);

        var first = node.Generate();
        var second = node.Generate();

        Assert.Equal(baseTime.ToUnixTimeMilliseconds(), node.GetTimeFromId(first));
        Assert.Equal(baseTime.AddMilliseconds(1).ToUnixTimeMilliseconds(), node.GetTimeFromId(second));
        Assert.True(second > first);
    }

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow()
        {
            return now;
        }
    }

    private sealed class QueueTimeProvider(params DateTimeOffset[] times) : TimeProvider
    {
        private readonly Queue<DateTimeOffset> _times = new(times);

        public override DateTimeOffset GetUtcNow()
        {
            return _times.Count > 1 ? _times.Dequeue() : _times.Peek();
        }
    }

    private sealed class SequenceOverflowTimeProvider(DateTimeOffset baseTime) : TimeProvider
    {
        public int CallCount { get; private set; }

        public override DateTimeOffset GetUtcNow()
        {
            CallCount++;
            return CallCount <= SnowflakeConstants.MaxSequence + 2
                ? baseTime
                : baseTime.AddMilliseconds(1);
        }
    }
}
