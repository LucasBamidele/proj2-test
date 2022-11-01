{{ config(materialized='table') }}

select * from graph.tweets limit 5