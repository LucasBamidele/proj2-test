
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT id, text FROM graph.tweets WHERE CONTAINS_SUBSTR(text, '#maga') or CONTAINS_SUBSTR(text, 'trump') 