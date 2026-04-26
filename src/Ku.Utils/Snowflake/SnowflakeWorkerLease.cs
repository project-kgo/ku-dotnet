namespace Ku.Utils.Snowflake;

internal readonly record struct SnowflakeWorkerLease(long WorkerId, long LastTimestamp);
