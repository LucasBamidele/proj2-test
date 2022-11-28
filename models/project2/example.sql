{{ config(materialized='table') }}
select * from graph.tweets limit 10
-- Select count(*) from `graph.tweets` where twitter_username like '%@%' limit 10
