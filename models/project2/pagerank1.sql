{{ config(materialized='table') }}
with nodes as
(
    select src as node from {{ref('q3')}}
    union distinct
    select dst as node from {{ref('q3')}} 
),
page_rank0 as ( 
    select src as sourc, count(src)/(select (count(*)) from nodes) as pr
    from {{ref('q3')}}
    group by src
),
page_rank as (
    select dst as username, sum(pr) as page_rank_score
    from {{ref('q3')}}, page_rank0
    where sourc = src
    group by dst
    order by page_rank_score desc
)
select * from page_rank
