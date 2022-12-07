{{ config(materialized='table') }}
-- select * from graph.tweets limit 10
-- Select count(*) from `graph.tweets` where twitter_username like '%@%' limit 10
-- select * from graph.tweets where text like concat('%',twitter_username,'%') limit 10
-- select 
WITH RECURSIVE
  T1 AS ( (SELECT 1 AS n) UNION ALL (SELECT n + 1 AS n FROM T1 WHERE n < 10) )
SELECT n FROM T1 order by n
