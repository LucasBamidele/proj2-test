{{ config(materialized='table') }}

with g as (select * from {{ref('q3')}}),
triangles as (select g1.src, g2.src, g3.src from g as g1, g as g2, g as g3
where g1.dst = g2.src and g2.dst = g3.src and g3.dst = g1.src 
and g1.src <> g2.src and g2.src <> g3.src and g1.src <> g3.src)
select count(*)/3 from triangles