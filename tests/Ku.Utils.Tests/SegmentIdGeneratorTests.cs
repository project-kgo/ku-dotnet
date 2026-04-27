using System.Collections.Concurrent;
using Ku.Utils.Segment;
using SegmentRange = Ku.Utils.Segment.Segment;

namespace Ku.Utils.Tests;

public sealed class SegmentIdGeneratorTests
{
    [Fact]
    public async Task CreateAsync_WhenConnectionStringIsEmpty_Throws()
    {
        var options = new SegmentIdGeneratorOptions
        {
            ConnectionString = " "
        };

        await Assert.ThrowsAsync<ArgumentException>(() => SegmentIdGenerator.CreateAsync(options));
    }

    [Theory]
    [InlineData("1invalid", "id_generator")]
    [InlineData("infra", "id-generator")]
    public void CreateForTests_WhenIdentifierIsInvalid_Throws(string schema, string tableName)
    {
        var store = new InMemorySegmentIdStore();
        var options = CreateOptions() with
        {
            Schema = schema,
            TableName = tableName
        };

        Assert.Throws<ArgumentException>(() => SegmentIdGenerator.CreateForTests(options, store));
    }

    [Fact]
    public void CreateForTests_WhenDefaultStepIsInvalid_Throws()
    {
        var store = new InMemorySegmentIdStore();
        var options = CreateOptions() with
        {
            DefaultStep = 0
        };

        Assert.Throws<ArgumentOutOfRangeException>(() => SegmentIdGenerator.CreateForTests(options, store));
    }

    [Theory]
    [InlineData(0D)]
    [InlineData(-0.1D)]
    [InlineData(1.1D)]
    [InlineData(double.NaN)]
    public void CreateForTests_WhenPreloadRatioIsInvalid_Throws(double preloadRatio)
    {
        var store = new InMemorySegmentIdStore();
        var options = CreateOptions() with
        {
            PreloadRatio = preloadRatio
        };

        Assert.Throws<ArgumentOutOfRangeException>(() => SegmentIdGenerator.CreateForTests(options, store));
    }

    [Fact]
    public void CreateForTests_WhenAsyncLoadTimeoutIsInvalid_Throws()
    {
        var store = new InMemorySegmentIdStore();
        var options = CreateOptions() with
        {
            AsyncLoadTimeout = TimeSpan.Zero
        };

        Assert.Throws<ArgumentOutOfRangeException>(() => SegmentIdGenerator.CreateForTests(options, store));
    }

    [Fact]
    public async Task InitAsync_WhenRecordIsNew_CreatesRecordAndLoadsFirstSegment()
    {
        var store = new InMemorySegmentIdStore();
        var generator = SegmentIdGenerator.CreateForTests(CreateOptions(), store);

        await generator.InitAsync(7, 100, 5);

        var record = store.GetRecord(7);
        Assert.Equal(105, record.MaxId);
        Assert.Equal(5, record.Step);
        Assert.Equal(1, record.EnsureCount);
        Assert.Equal(1, record.FetchCount);
        Assert.Equal(101, await generator.GetIdAsync(7));
    }

    [Fact]
    public async Task InitAsync_WhenRecordExists_UpdatesStepWithoutResettingMaxId()
    {
        var store = new InMemorySegmentIdStore();
        var generator = SegmentIdGenerator.CreateForTests(CreateOptions() with { PreloadRatio = 1D }, store);

        await generator.InitAsync(3, 100, 5);
        await generator.InitAsync(3, 0, 10);

        var record = store.GetRecord(3);
        Assert.Equal(105, record.MaxId);
        Assert.Equal(10, record.Step);
        Assert.Equal(2, record.EnsureCount);
        Assert.Equal(1, record.FetchCount);
    }

    [Fact]
    public async Task GetIdAsync_WhenBizTagIsUnknown_InitializesWithDefaultOptions()
    {
        var store = new InMemorySegmentIdStore();
        var generator = SegmentIdGenerator.CreateForTests(CreateOptions() with
        {
            DefaultStartId = 10,
            DefaultStep = 4,
            PreloadRatio = 1D
        }, store);

        var id = await generator.GetIdAsync(9);

        var record = store.GetRecord(9);
        Assert.Equal(11, id);
        Assert.Equal(14, record.MaxId);
        Assert.Equal(4, record.Step);
    }

    [Fact]
    public async Task GetIdAsync_WhenCurrentSegmentIsExhausted_SwitchesToNextSegment()
    {
        var store = new InMemorySegmentIdStore();
        var generator = SegmentIdGenerator.CreateForTests(CreateOptions() with
        {
            DefaultStep = 3,
            PreloadRatio = 1D
        }, store);

        var ids = new List<long>();
        for (var index = 0; index < 6; index++)
        {
            ids.Add(await generator.GetIdAsync(1));
        }

        Assert.Equal([1, 2, 3, 4, 5, 6], ids);
        Assert.True(store.GetRecord(1).FetchCount >= 2);
    }

    [Fact]
    public async Task GetIdAsync_WhenPreloadRatioIsReached_LoadsNextSegmentAsynchronously()
    {
        var store = new InMemorySegmentIdStore();
        var generator = SegmentIdGenerator.CreateForTests(CreateOptions() with
        {
            DefaultStep = 10,
            PreloadRatio = 0.2D
        }, store);

        Assert.Equal(1, await generator.GetIdAsync(1));
        Assert.Equal(1, store.GetRecord(1).FetchCount);

        Assert.Equal(2, await generator.GetIdAsync(1));
        await WaitUntilAsync(() => store.GetRecord(1).FetchCount >= 2);

        Assert.Equal(2, store.GetRecord(1).FetchCount);
    }

    [Fact]
    public async Task GetIdAsync_WhenAsyncPreloadFails_FetchesSynchronouslyAfterExhaustion()
    {
        var store = new InMemorySegmentIdStore();
        var generator = SegmentIdGenerator.CreateForTests(CreateOptions() with
        {
            DefaultStep = 3,
            PreloadRatio = 0.34D
        }, store);

        Assert.Equal(1, await generator.GetIdAsync(1));
        store.FailNextFetch();
        Assert.Equal(2, await generator.GetIdAsync(1));

        await WaitUntilAsync(() => store.FailedFetchCount == 1);

        Assert.Equal(3, await generator.GetIdAsync(1));
        Assert.Equal(1, store.GetRecord(1).FetchCount);
        Assert.Equal(4, await generator.GetIdAsync(1));
        Assert.Equal(2, store.GetRecord(1).FetchCount);
        Assert.Equal(1, store.FailedFetchCount);
    }

    [Fact]
    public async Task Dispose_WhenPreloadIsRunning_CancelsPreloadTask()
    {
        var store = new InMemorySegmentIdStore();
        var generator = SegmentIdGenerator.CreateForTests(CreateOptions() with
        {
            DefaultStep = 10,
            PreloadRatio = 0.2D,
            AsyncLoadTimeout = TimeSpan.FromMinutes(1)
        }, store);

        Assert.Equal(1, await generator.GetIdAsync(1));
        store.DelayNextFetch();
        Assert.Equal(2, await generator.GetIdAsync(1));

        await WaitUntilAsync(() => store.DelayedFetchStartedCount == 1);

        generator.Dispose();
        generator.Dispose();

        Assert.Equal(1, store.CanceledFetchCount);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => generator.GetIdAsync(1));
    }

    [Fact]
    public async Task GetIdAsync_WhenCalledConcurrentlyForSameBizTag_ReturnsUniqueIds()
    {
        var store = new InMemorySegmentIdStore();
        var generator = SegmentIdGenerator.CreateForTests(CreateOptions() with
        {
            DefaultStep = 200,
            PreloadRatio = 1D
        }, store);

        var tasks = Enumerable.Range(0, 100)
            .Select(_ => generator.GetIdAsync(1))
            .ToArray();
        var ids = await Task.WhenAll(tasks);

        Assert.Equal(100, ids.Distinct().Count());
        Assert.Equal(Enumerable.Range(1, 100).Select(static value => (long)value), ids.Order());
    }

    [Fact]
    public async Task GetIdAsync_WhenCalledForDifferentBizTags_UsesIndependentSegments()
    {
        var store = new InMemorySegmentIdStore();
        var generator = SegmentIdGenerator.CreateForTests(CreateOptions() with
        {
            DefaultStep = 2,
            PreloadRatio = 1D
        }, store);

        var first = await generator.GetIdAsync(1);
        var second = await generator.GetIdAsync(2);

        Assert.Equal(1, first);
        Assert.Equal(1, second);
        Assert.Equal(2, store.GetRecord(1).MaxId);
        Assert.Equal(2, store.GetRecord(2).MaxId);
    }

    private static SegmentIdGeneratorOptions CreateOptions()
    {
        return new SegmentIdGeneratorOptions
        {
            ConnectionString = "Host=localhost;Database=ku_test;Username=ku;Password=secret"
        };
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        while (!condition())
        {
            await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationTokenSource.Token);
        }
    }

    private sealed class InMemorySegmentIdStore : ISegmentIdStore
    {
        private readonly ConcurrentDictionary<int, Record> _records = new();
        private readonly Lock _lock = new();
        private int _failNextFetch;
        private int _delayNextFetch;
        private int _canceledFetchCount;
        private int _delayedFetchStartedCount;

        public int FailedFetchCount { get; private set; }

        public int CanceledFetchCount => Volatile.Read(ref _canceledFetchCount);

        public int DelayedFetchStartedCount => Volatile.Read(ref _delayedFetchStartedCount);

        public Task EnsureTableAndRecordAsync(int bizTag, long startId, int step, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            lock (_lock)
            {
                var record = _records.GetOrAdd(bizTag, _ => new Record(startId, step));
                record.Step = step;
                record.EnsureCount++;
            }

            return Task.CompletedTask;
        }

        public Task<SegmentRange> FetchSegmentAsync(int bizTag, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (Interlocked.Exchange(ref _delayNextFetch, 0) == 1)
            {
                return FetchDelayedSegmentAsync(bizTag, cancellationToken);
            }

            lock (_lock)
            {
                if (Interlocked.Exchange(ref _failNextFetch, 0) == 1)
                {
                    FailedFetchCount++;
                    throw new InvalidOperationException("模拟领取号段失败。");
                }

                if (!_records.TryGetValue(bizTag, out var record))
                {
                    throw new InvalidOperationException($"biz_tag {bizTag} 不存在。");
                }

                record.MaxId += record.Step;
                record.FetchCount++;

                var start = record.MaxId - record.Step + 1L;
                return Task.FromResult(new SegmentRange(start, record.MaxId));
            }
        }

        public void FailNextFetch()
        {
            Interlocked.Exchange(ref _failNextFetch, 1);
        }

        public void DelayNextFetch()
        {
            Interlocked.Exchange(ref _delayNextFetch, 1);
        }

        public RecordSnapshot GetRecord(int bizTag)
        {
            lock (_lock)
            {
                var record = _records[bizTag];
                return new RecordSnapshot(record.MaxId, record.Step, record.EnsureCount, record.FetchCount);
            }
        }

        private sealed class Record(long maxId, int step)
        {
            public long MaxId { get; set; } = maxId;

            public int Step { get; set; } = step;

            public int EnsureCount { get; set; }

            public int FetchCount { get; set; }
        }

        private async Task<SegmentRange> FetchDelayedSegmentAsync(int bizTag, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _delayedFetchStartedCount);

            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                Interlocked.Increment(ref _canceledFetchCount);
                throw;
            }

            return await FetchSegmentAsync(bizTag, cancellationToken);
        }
    }

    private readonly record struct RecordSnapshot(long MaxId, int Step, int EnsureCount, int FetchCount);
}
