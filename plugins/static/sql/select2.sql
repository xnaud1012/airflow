
WITH CTE AS(
SELECT SUBSTR(json_agg, 1, LENGTH(json_agg) - 1) AS HEAD_INFO
FROM (
    SELECT LISTAGG(XNAUD.JSON_OBJECT(column_name, comments), ',')
    WITHIN GROUP (ORDER BY column_name) AS json_agg
    FROM all_col_comments
    WHERE table_name = 'MSEPMRID'
)
)
SELECT cte.HEAD_INFO, m.* FROM CTE
INNER JOIN 
MSEPMRID m ON 1=1 ;
