namespace Ku.Utils.Segment;

internal sealed class SegmentBuffer(int bizTag, ISegmentIdStore store, SegmentIdGeneratorOptions options) : IDisposable
{
    private readonly ISegmentIdStore _store = store;
    private readonly SegmentIdGeneratorOptions _options = options;
    private readonly SemaphoreSlim _stateLock = new(1, 1);
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly Lock _disposeLock = new();
    private Segment? _current;
    private Segment? _next;
    private Task<bool>? _loadTask;
    private CancellationTokenSource? _loadCancellationTokenSource;
    private bool _isNextReady;
    private bool _isInitialized;
    private bool _preloadStartedForCurrent;
    private bool _disposed;

    public int BizTag { get; } = bizTag;

    public int Step { get; private set; } = options.DefaultStep;

    public async Task InitializeAsync(long startId, int step, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        SegmentIdGenerator.ValidateStep(step, nameof(step));

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            await _store.EnsureTableAndRecordAsync(BizTag, startId, step, cancellationToken);

            await _stateLock.WaitAsync(cancellationToken);
            try
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                Step = step;
                _isInitialized = true;
            }
            finally
            {
                _stateLock.Release();
            }

            await LoadCurrentSegmentAsync(cancellationToken);
        }
        finally
        {
            _initLock.Release();
        }
    }

    public async Task<long> NextIdAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_isInitialized)
        {
            await InitializeAsync(_options.DefaultStartId, _options.DefaultStep, cancellationToken);
        }

        while (true)
        {
            await _stateLock.WaitAsync(cancellationToken);
            try
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                _current ??= await _store.FetchSegmentAsync(BizTag, cancellationToken);

                if (_current.Current > _current.End)
                {
                    if (_isNextReady && _next is not null)
                    {
                        SwitchToNextSegment();
                    }
                    else if (_loadTask is { IsCompleted: false } loadTask)
                    {
                        _stateLock.Release();

                        try
                        {
                            await loadTask.WaitAsync(cancellationToken);
                        }
                        finally
                        {
                            await _stateLock.WaitAsync(CancellationToken.None);
                        }

                        continue;
                    }
                    else
                    {
                        _current = await _store.FetchSegmentAsync(BizTag, cancellationToken);
                        _preloadStartedForCurrent = false;
                    }
                }

                var id = _current.Current;
                _current.Current++;

                StartPreloadIfNeeded(id);

                return id;
            }
            finally
            {
                _stateLock.Release();
            }
        }
    }

    private async Task LoadCurrentSegmentAsync(CancellationToken cancellationToken)
    {
        await _stateLock.WaitAsync(cancellationToken);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_current is { } current && current.Current <= current.End)
            {
                return;
            }

            if (_isNextReady && _next is not null)
            {
                SwitchToNextSegment();
                return;
            }

            _current = await _store.FetchSegmentAsync(BizTag, cancellationToken);
            _preloadStartedForCurrent = false;
        }
        finally
        {
            _stateLock.Release();
        }
    }

    private void StartPreloadIfNeeded(long issuedId)
    {
        if (_disposed || _current is null || _preloadStartedForCurrent || _isNextReady || _loadTask is { IsCompleted: false })
        {
            return;
        }

        var total = _current.End - _current.Start + 1L;
        var used = issuedId - _current.Start + 1L;

        if (total <= 0 || used / (double)total < _options.PreloadRatio)
        {
            return;
        }

        _preloadStartedForCurrent = true;
        _loadCancellationTokenSource = new CancellationTokenSource(_options.AsyncLoadTimeout);
        _loadTask = LoadNextSegmentAsync(_loadCancellationTokenSource);
    }

    private async Task<bool> LoadNextSegmentAsync(CancellationTokenSource cancellationTokenSource)
    {
        try
        {
            var segment = await _store.FetchSegmentAsync(BizTag, cancellationTokenSource.Token);

            await _stateLock.WaitAsync(CancellationToken.None);
            try
            {
                if (_disposed)
                {
                    return false;
                }

                _next = segment;
                _isNextReady = true;
            }
            finally
            {
                _stateLock.Release();
            }

            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch
        {
            return false;
        }
        finally
        {
            await _stateLock.WaitAsync(CancellationToken.None);
            try
            {
                if (ReferenceEquals(_loadCancellationTokenSource, cancellationTokenSource))
                {
                    _loadCancellationTokenSource = null;
                }
            }
            finally
            {
                _stateLock.Release();
                cancellationTokenSource.Dispose();
            }
        }
    }

    private void SwitchToNextSegment()
    {
        _current = _next;
        _next = null;
        _isNextReady = false;
        _preloadStartedForCurrent = false;
    }

    public void Dispose()
    {
        Task<bool>? loadTask;
        CancellationTokenSource? loadCancellationTokenSource;

        lock (_disposeLock)
        {
            if (_disposed)
            {
                return;
            }

            _stateLock.Wait();
            try
            {
                _disposed = true;
                loadTask = _loadTask;
                loadCancellationTokenSource = _loadCancellationTokenSource;
                loadCancellationTokenSource?.Cancel();
            }
            finally
            {
                _stateLock.Release();
            }

            if (loadTask is not null)
            {
                try
                {
                    loadTask.GetAwaiter().GetResult();
                }
                catch (OperationCanceledException)
                {
                }
            }

            _stateLock.Dispose();
            _initLock.Dispose();
        }
    }
}
