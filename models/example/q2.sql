{{ config(materialized='table') }}
with p1 as (select (PARSE_DATETIME('%c', REPLACE(create_time, ' +0000', ''))) as dt from graph.tweets where CONTAINS_SUBSTR(text, 'maga'))
select EXTRACT(MONTH from dt) as month, EXTRACT(YEAR from dt) as year, count(*) as count from p1 group by month, year;
-- select FORMAT_DATE('%b %Y', CAST(create_time as DATETIME)) as p from graph.tweets limit 5;

-- select (SUBSTR(create_time, -4)), CAST(SUBSTR(create_time, 5, 3), MONTH), create_time from graph.tweets limit 5
-- select CAST(create_time as DATETIME) from graph.tweets limit 5;
-- select EXTRACT(MONTH from create_time) from graph.tweets limit 5;
-- select something as year, EXTRACT(MONTH from create_time) as month, count(*) from graph.tweets group by something, s2