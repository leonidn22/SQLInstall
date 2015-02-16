-- Set the new value:

--- Max partitions per table (default 1024)
SELECT SET_CONFIG_PARAMETER('MaxPartitionCount', 2048);
--- Max Client Sessions per node (default 60)
SELECT SET_CONFIG_PARAMETER('MaxClientSessions', 200);
--- Max memory for optimizer (default 100)
SELECT SET_CONFIG_PARAMETER('MaxOptMemMB', 300);

