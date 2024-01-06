-- pulls the layers and titles in the raw data
{%- set titles_query -%}
SELECT DISTINCT LAYER, UPPER(REPLACE(TRIM(TITLE), ' ', '_')) AS TITLE FROM {{ ref("stg_gsheets_hierarchy_table") }} ORDER BY LAYER DESC
{%- endset -%}

-- this sets the dictionary that will help fill the query
{%- set layer_titles = {} -%}
{%- for row in run_query(titles_query) -%}
    {%- set layer = row[0] -%}
    {%- set title = row[1] -%}
    {%- do layer_titles.update({layer: title}) -%}
{%- endfor -%}

-- pulls the highest layer from the raw data for the subsequent loops
{%- set sql_statement -%}
SELECT MAX(LAYER) FROM {{ ref("stg_gsheets_hierarchy_table") }}
{%- endset -%}

-- sets the max_layer to the highest value in the raw data, which is associated with Watchers
{%- set max_layer = dbt_utils.get_single_value(sql_statement, default="0") | int -%}

-- this recursive CTE grabs the data and pairs each employee with their direct manager
WITH RECURSIVE
    hierarchy_cte AS (
        -- Anchor: select the top of the hierarchy
        SELECT
            ID,
            NAME,
            TITLE,
            LAYER,
            MANAGER_ID,
            START_DATE,
            END_DATE,
            -- loop through the layers and construct the columns needed based on the number of layers retrieved from the max_layer macro
            {%- for i in range(1, max_layer + 1) %}
            CASE WHEN LAYER = {{ i }} THEN ID END AS LAYER_{{ i }}_ID,
            CASE WHEN LAYER = {{ i }} THEN NAME END AS LAYER_{{ i }}_NAME
            {%- if not loop.last %},{%- endif %}
            {%- endfor %}
            -- in the line prior to the endfor, add a comma until the last iteration in the loop
            -- the dash after % removes white space, putting the comma on the same line as the row above
        FROM {{ ref("stg_gsheets_hierarchy_table") }}
        WHERE LAYER = 1
        UNION ALL
        -- Recursive: select subordinates of the previous level
        SELECT
            sh.ID,
            sh.NAME,
            sh.TITLE,
            sh.LAYER,
            sh.MANAGER_ID,
            sh.START_DATE,
            sh.END_DATE,
            {%- for i in range(1, max_layer + 1) %}
            COALESCE(
                shc.LAYER_{{ i }}_ID,
                CASE WHEN sh.LAYER = {{ i }} THEN sh.ID END
            ),
            COALESCE(
                shc.LAYER_{{ i }}_NAME,
                CASE WHEN sh.LAYER = {{ i }} THEN sh.NAME END
            )
            {%- if not loop.last %},{%- endif %}
            {%- endfor %}
        FROM {{ ref("stg_gsheets_hierarchy_table") }} sh
        JOIN
            hierarchy_cte shc
            ON sh.MANAGER_ID = shc.ID
            AND sh.START_DATE >= shc.START_DATE
            AND sh.END_DATE <= shc.END_DATE
    ),

    organization_table AS (
        SELECT
            {%- for i in range(max_layer, 0, -1) %}
            MAX(LAYER_{{ i }}_ID) OVER (
                PARTITION BY LAYER_{{ max_layer }}_ID, START_DATE, END_DATE
            ) AS {{ layer_titles[i] }}_ID,
            MAX(LAYER_{{ i }}_NAME) OVER (
                PARTITION BY LAYER_{{ max_layer }}_ID, START_DATE, END_DATE
            ) AS {{ layer_titles[i] }}_NAME
            {%- if not loop.last %},{%- endif -%}
            {%- if loop.first %}
            MAX(START_DATE) OVER (
                PARTITION BY LAYER_{{ i }}_ID, START_DATE, END_DATE
            ) AS START_DATE,
            MAX(END_DATE) OVER (
                PARTITION BY LAYER_{{ i }}_ID, START_DATE, END_DATE
            ) AS END_DATE,
            CASE
                WHEN
                    MAX(START_DATE) OVER (
                        PARTITION BY LAYER_{{ i }}_ID, START_DATE, END_DATE
                    )
                    <= CURRENT_DATE
                    AND MAX(END_DATE) OVER (
                        PARTITION BY LAYER_{{ i }}_ID, START_DATE, END_DATE
                    )
                    >= CURRENT_DATE
                THEN 1
                ELSE 0
            END AS CURRENT_RELATIONSHIP_FLAG,
            {%- endif -%}
            {%- endfor %}
        FROM hierarchy_cte
        ORDER BY {{ layer_titles[max_layer] }}_NAME ASC, START_DATE ASC
    )

select
    {{ layer_titles[max_layer] }}_ID,
    -- this allows us to alter the titles in the underlying raw data without having to update this model
    {{ layer_titles[max_layer] }}_NAME,
    START_DATE,
    END_DATE,
    CURRENT_RELATIONSHIP_FLAG,
    {%- for i in range(max_layer - 1, 0, -1) %}
    {{ layer_titles[i] }}_ID,
    {{ layer_titles[i] }}_NAME
    {%- if not loop.last %},{%- endif %}
    {%- endfor %}
FROM organization_table
WHERE {{ layer_titles[max_layer] }}_ID IS NOT null
ORDER BY {{ layer_titles[max_layer] }}_NAME ASC, START_DATE ASC
