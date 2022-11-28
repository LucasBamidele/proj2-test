{{ config(materialized='table') }}


with indegree as (
    SELECT dst , count(*) as indeg 
    FROM {{ref('q3')}} 
    GROUP BY dst
    ORDER BY count(*) desc
),
likes as (
    select twitter_username, avg(like_num) as avg_likes from graph.tweets group by twitter_username
),
unpopular_users as (
    select dst as unpopular from indegree, likes where 
    twitter_username = dst and 
    (indeg < (select avg(indeg) from indegree)) and 
    (avg_likes < (select avg(avg_likes) from likes))
),
popular_users as (
    select dst as popular from indegree, likes where 
    twitter_username = dst and 
    (indeg >= (select avg(indeg) from indegree)) and 
    (avg_likes >= (select avg(avg_likes) from likes))
),
unpopular_tweets as (
    select text
    from unpopular_users, graph.tweets
    where twitter_username = unpopular
)
select count(*) /(select count(*) from unpopular_tweets) from unpopular_tweets, popular_users 
-- select text, popular from unpopular_tweets, popular_users 
where text like concat('%@',popular,'%')
limit 200
