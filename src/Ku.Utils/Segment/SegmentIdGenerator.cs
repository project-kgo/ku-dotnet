using System.Collections.Concurrent;
using Ku.Utils.Database.PostgreSql;

namespace Ku.Utils.Segment;

/// <summary>
/// 基于 PostgreSQL 号段的分布式 ID 生成器。
/// </summary>
public sealed class SegmentIdGenerator : IDisposable
{
    private readonly ConcurrentDictionary<int, SegmentBuffer> _buffers = new();
    private readonly ISegmentIdStore _store;
    private readonly SegmentIdGeneratorOptions _options;
    private bool _disposed;

    private SegmentIdGenerator(SegmentIdGeneratorOptions options, ISegmentIdStore store)
    {
        ValidateOptions(options);

        _options = options;
        _store = store;
    }

    /// <summary>
    /// 创建 Segment ID 生成器。
    /// </summary>
    /// <param name="options">生成器配置。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>Segment ID 生成器。</returns>
    public static Task<SegmentIdGenerator> CreateAsync(
        SegmentIdGeneratorOptions options,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(options);
        ValidateOptions(options);

        var dataSource = PostgreSqlDataSourceFactory.GetOrCreate(options.ToPostgreSqlConnectionOptions());
        var store = new PostgreSqlSegmentIdStore(dataSource, options.Schema, options.TableName);

        return Task.FromResult(new SegmentIdGenerator(options, store));
    }

    /// <summary>
    /// 初始化指定业务标识的号段配置。
    /// </summary>
    /// <param name="bizTag">业务标识。</param>
    /// <param name="startId">记录不存在时使用的起始 ID。</param>
    /// <param name="step">每次领取的号段步长。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    public Task InitAsync(
        int bizTag,
        long startId = 0,
        int step = 1000,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ValidateStep(step, nameof(step));

        var buffer = GetOrCreateBuffer(bizTag);
        return buffer.InitializeAsync(startId, step, cancellationToken);
    }

    /// <summary>
    /// 获取指定业务标识的下一个 ID。
    /// </summary>
    /// <param name="bizTag">业务标识。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>下一个唯一 ID。</returns>
    public async Task<long> GetIdAsync(int bizTag, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var buffer = GetOrCreateBuffer(bizTag);
        return await buffer.NextIdAsync(cancellationToken);
    }

    /// <summary>
    /// 关闭生成器。连接由共享 PostgreSQL 数据源管理，此方法不释放连接池。
    /// </summary>
    public void Close()
    {
        Dispose();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        foreach (var buffer in _buffers.Values)
        {
            buffer.Dispose();
        }
    }

    internal static SegmentIdGenerator CreateForTests(SegmentIdGeneratorOptions options, ISegmentIdStore store)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(store);

        return new SegmentIdGenerator(options, store);
    }

    internal static void ValidateStep(int step, string argumentName)
    {
        if (step <= 0)
        {
            throw new ArgumentOutOfRangeException(argumentName, "号段步长必须大于 0。");
        }
    }

    private SegmentBuffer GetOrCreateBuffer(int bizTag)
    {
        return _buffers.GetOrAdd(bizTag, static (key, state) => new SegmentBuffer(key, state.Store, state.Options), (Store: _store, Options: _options));
    }

    private static void ValidateOptions(SegmentIdGeneratorOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("PostgreSQL 连接字符串不能为空。", nameof(options));
        }

        PostgreSqlIdentifier.Quote(options.Schema, nameof(options.Schema));
        PostgreSqlIdentifier.Quote(options.TableName, nameof(options.TableName));
        ValidateStep(options.DefaultStep, nameof(options.DefaultStep));

        if (options.PreloadRatio is <= 0D or > 1D || double.IsNaN(options.PreloadRatio))
        {
            throw new ArgumentOutOfRangeException(nameof(options), "预加载比例必须大于 0 且小于等于 1。");
        }

        if (options.AsyncLoadTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "异步预加载超时时间必须大于 0。");
        }
    }
}
