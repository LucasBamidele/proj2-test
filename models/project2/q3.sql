
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with tmp as (select twitter_username as src, REGEXP_EXTRACT(text, r'@[\w\d]+') as dst from graph.tweets)
select distinct src, SUBSTR(dst, 1) as dst from tmp where STARTS_WITH(dst, '@')