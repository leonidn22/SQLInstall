CREATE or REPLACE VIEW public.who AS
-- who is running now
 SELECT s.session_id,
        s.user_name,
        s.client_hostname,
        "datediff"('ss'::varchar(2), s.statement_start, (statement_timestamp())::timestamp) AS duration_sec,
        substr(regexp_replace(s.current_statement, E'[\\r\\t\\f\\n]'::varchar(10), ' '::varchar(1), 1, 0, ''::varchar), 1, 200) AS current_statement
 FROM v_monitor.sessions s
 WHERE ((s.current_statement <> ''::varchar) AND (s.session_id <> ( SELECT current_session.session_id
 FROM v_monitor.current_session)));


