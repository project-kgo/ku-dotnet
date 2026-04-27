namespace Ku.Utils.Segment;

internal interface ISegmentIdStore
{
    Task EnsureTableAndRecordAsync(int bizTag, long startId, int step, CancellationToken cancellationToken);

    Task<Segment> FetchSegmentAsync(int bizTag, CancellationToken cancellationToken);
}
