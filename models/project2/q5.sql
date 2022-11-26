{{ config(materialized='table') }}


with indegree as (
    SELECT dst , count(*) as indeg 
    FROM {{ref('q3')}} 
    GROUP BY dst
    ORDER BY count(*) desc
),
likes as (
    select twitter_username, avg(like_num) as avg_likes from graph.tweets group by twitter_username
)
select dst, indeg, avg_likes from indegree, likes where 
twitter_username = dst and 
(indeg < (select avg(indeg) from indegree)) and 
(avg_likes < (select avg(avg_likes) from likes))