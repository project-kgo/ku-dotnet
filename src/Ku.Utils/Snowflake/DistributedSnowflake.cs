using Ku.Utils.Database.PostgreSql;
using Microsoft.Extensions.Logging;

namespace Ku.Utils.Snowflake;

/// <summary>
/// 基于 PostgreSQL worker 租约的分布式 Snowflake ID 生成器。
/// </summary>
public sealed class DistributedSnowflake : IDisposable, IAsyncDisposable
{
    private static readonly Action<ILogger, Exception?> HeartbeatFailed =
        LoggerMessage.Define(LogLevel.Error, new EventId(1, nameof(HeartbeatFailed)), "Snowflake heartbeat failed.");

    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly TimeProvider _timeProvider;
    private readonly PostgreSqlSnowflakeWorkerStore _workerStore;
    private readonly SnowflakeNode _node;
    private readonly TimeSpan _heartbeatInterval;
    private readonly TimeSpan _safetyThreshold;
    private readonly ILogger? _logger;
    private readonly Task _heartbeatTask;
    private long _lastHeartbeatTicks;
    private bool _disposed;

    private DistributedSnowflake(
        SnowflakeNode node,
        PostgreSqlSnowflakeWorkerStore workerStore,
        TimeProvider timeProvider,
        TimeSpan heartbeatInterval,
        TimeSpan safetyThreshold,
        ILogger? logger)
    {
        _node = node;
        _workerStore = workerStore;
        _timeProvider = timeProvider;
        _heartbeatInterval = heartbeatInterval;
        _safetyThreshold = safetyThreshold;
        _logger = logger;
        _lastHeartbeatTicks = timeProvider.GetUtcNow().UtcTicks;
        _heartbeatTask = RunHeartbeatAsync(_cancellationTokenSource.Token);
    }

    /// <summary>
    /// 当前实例持有的 workerId。
    /// </summary>
    public long WorkerId => _node.WorkerId;

    /// <summary>
    /// 创建分布式 Snowflake ID 生成器。
    /// </summary>
    /// <param name="options">生成器配置。</param>
    /// <param name="timeProvider">时间提供器，未传入时使用系统时间。</param>
    /// <param name="logger">可选日志记录器。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>分布式 Snowflake ID 生成器。</returns>
    public static async Task<DistributedSnowflake> CreateAsync(
        DistributedSnowflakeOptions options,
        TimeProvider? timeProvider = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ValidateOptions(options);

        timeProvider ??= TimeProvider.System;
        var dataSource = PostgreSqlDataSourceFactory.GetOrCreate(options.ToPostgreSqlConnectionOptions());
        var workerStore = new PostgreSqlSnowflakeWorkerStore(dataSource, options.Schema, options.TableName, timeProvider);

        await workerStore.EnsureTableAsync(cancellationToken);
        var lease = await workerStore.AllocateWorkerAsync(options.WorkerTimeout, cancellationToken);

        await WaitForClockCatchUpAsync(lease.LastTimestamp, options.MaximumStartupClockDrift, timeProvider, cancellationToken);

        var node = new SnowflakeNode(lease.WorkerId, options.Epoch, timeProvider);

        return new DistributedSnowflake(
            node,
            workerStore,
            timeProvider,
            options.HeartbeatInterval,
            options.SafetyThreshold,
            logger);
    }

    /// <summary>
    /// 生成一个 Snowflake ID。
    /// </summary>
    /// <returns>64 位 Snowflake ID。</returns>
    public long Generate()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var lastHeartbeat = new DateTimeOffset(Volatile.Read(ref _lastHeartbeatTicks), TimeSpan.Zero);
        if (_timeProvider.GetUtcNow() - lastHeartbeat > _safetyThreshold)
        {
            throw new InvalidOperationException("Snowflake worker 租约已过期，可能存在 split-brain 风险，已拒绝生成 ID。");
        }

        return _node.Generate();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        await _cancellationTokenSource.CancelAsync();

        try
        {
            await _heartbeatTask;
        }
        catch (OperationCanceledException)
        {
        }

        _cancellationTokenSource.Dispose();
    }

    private async Task RunHeartbeatAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_heartbeatInterval, _timeProvider);

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                await UpdateHeartbeatAsync(cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task UpdateHeartbeatAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _workerStore.UpdateHeartbeatAsync(WorkerId, cancellationToken);
            Volatile.Write(ref _lastHeartbeatTicks, _timeProvider.GetUtcNow().UtcTicks);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            if (_logger is not null)
            {
                HeartbeatFailed(_logger, exception);
            }
        }
    }

    private static async Task WaitForClockCatchUpAsync(
        long lastTimestamp,
        TimeSpan maximumStartupClockDrift,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        var currentTimestamp = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        if (lastTimestamp <= currentTimestamp)
        {
            return;
        }

        var waitTime = TimeSpan.FromMilliseconds(lastTimestamp - currentTimestamp);
        if (waitTime > maximumStartupClockDrift)
        {
            throw new InvalidOperationException($"系统时钟回拨 {waitTime}，拒绝启动 Snowflake。");
        }

        await Task.Delay(waitTime + TimeSpan.FromMilliseconds(1), timeProvider, cancellationToken);
    }

    private static void ValidateOptions(DistributedSnowflakeOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("PostgreSQL 连接字符串不能为空。", nameof(options));
        }

        if (options.HeartbeatInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "心跳间隔必须大于 0。");
        }

        if (options.WorkerTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "worker 超时时间必须大于 0。");
        }

        if (options.SafetyThreshold <= TimeSpan.Zero || options.SafetyThreshold >= options.WorkerTimeout)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "安全阈值必须大于 0 且小于 worker 超时时间。");
        }

        if (options.MaximumStartupClockDrift < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "最大启动时钟漂移不能小于 0。");
        }
    }
}
