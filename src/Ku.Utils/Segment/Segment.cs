namespace Ku.Utils.Segment;

internal sealed class Segment(long start, long end)
{
    public long Start { get; } = start;

    public long End { get; } = end;

    public long Current { get; set; } = start;
}
