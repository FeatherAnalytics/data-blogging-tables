SELECT
        _ROW
    ,   ID
    ,   NAME
    ,   TITLE
    ,   LAYER
    ,   MANAGER_ID
    ,   PARSE_DATE('%m/%d/%Y', START_DATE) AS START_DATE
    ,   PARSE_DATE('%m/%d/%Y', END_DATE) AS END_DATE
    ,   _FIVETRAN_SYNCED
FROM
        {{ source("gsheets","hierarchy_table") }}