-- spark
SELECT get_json_object(payload, '$.kind') AS kind, count(*) AS n, sum(CAST(get_json_object(payload, '$.qty') AS INT)) AS qty
FROM events GROUP BY get_json_object(payload, '$.kind') ORDER BY kind;
-- native
SELECT json_extract_string(payload, '$.kind') AS kind, count(*) AS n, sum(CAST(json_extract_string(payload, '$.qty') AS INT)) AS qty
FROM events GROUP BY json_extract_string(payload, '$.kind') ORDER BY kind;
