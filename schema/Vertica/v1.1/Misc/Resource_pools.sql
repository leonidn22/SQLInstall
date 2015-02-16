create resource pool leo_pool_query
priority 30
memorysize '0%'
MAXMEMORYSIZE '95%'
plannedconcurrency 4
maxconcurrency 4
executionparallelism AUTO
queuetimeout 3600
RUNTIMEPRIORITY MEDIUM
RUNTIMEPRIORITYTHRESHOLD 15;

-- pools
alter resource pool general MAXMEMORYSIZE '95%';
alter resource pool general EXECUTIONPARALLELISM 33;
alter resource pool general PLANNEDCONCURRENCY 12;




