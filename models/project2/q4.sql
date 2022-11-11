{{ config(materialized='table') }}

SELECT (SELECT dst FROM {{ref('q3')}} GROUP BY dst ORDER BY count(*) desc LIMIT 1) as max_indegree,
       (SELECT src FROM {{ref('q3')}} GROUP BY src ORDER BY count(*) desc LIMIT 1) as max_outdegree