

CREATE OR REPLACE  VIEW public.compr AS
 SELECT projection_storage.projection_schema,
        projection_storage.projection_name,
        (sum((projection_storage.used_bytes / 1073741824::float)))::int AS GB,
        sum(projection_storage.row_count) AS rows,
        ((sum(projection_storage.used_bytes) / sum(projection_storage.row_count)))::numeric(10,2) AS "bytes/row",
        ((((sum(projection_storage.used_bytes) / sum(projection_storage.row_count)) * 10000000::numeric(18,0)) / 1048576::float))::numeric(10,2) AS "MB/10m_file",
        (((((sum(projection_storage.used_bytes) / sum(projection_storage.row_count)) * 10000000::numeric(18,0)) / 1048576::float) / 40::float))::numeric(10,2) AS vs_OTDF_GZIP
 FROM v_monitor.projection_storage
 WHERE ((projection_storage.anchor_table_name ~~* 'testresults'::varchar(11)) AND (projection_storage.projection_schema = 'public'::varchar(6)))
 GROUP BY projection_storage.projection_schema,
          projection_storage.projection_name;

