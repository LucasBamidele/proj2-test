with recursive 
outdegree as 
(
    select src as node, count(src) as outdeg
    from {{ref('q3')}}
    group by src
    union distinct
    select dst as node, 0 as outdeg
    from {{ref('q3')}}
    where dst not in (select src from {{ref('q3')}})

),
page_rank as (
    select node, 0 as iteration, 1/(select count(*) from outdegree) as partial_rank, outdeg
    from outdegree
    union ALL
    select graph.dst as node, pr.iteration +1 as iteration, pr.partial_rank/pr.outdeg as partial_rank, pr.outdeg as outdeg
    from {{ref('q3')}} graph, page_rank pr
    where pr.iteration < 2 and pr.node = graph.src
), 
ans as (
select node as username, sum(partial_rank) as page_rank_score
from page_rank
where iteration = 1
group by username
order by page_rank_score DESC
limit 20)

(SELECT *
FROM {{ref('pagerank2')}}
EXCEPT DISTINCT
SELECT *
FROM ans)
UNION ALL
(SELECT *
FROM ans
EXCEPT DISTINCT
SELECT *
FROM {{ref('pagerank2')}})