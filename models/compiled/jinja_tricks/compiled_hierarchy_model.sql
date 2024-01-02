-- pulls the layers and titles in the raw data-- this sets the dictionary that will help fill the query-- pulls the highest layer from the raw data for the subsequent loops-- sets the max_layer to the highest value in the raw data, which is associated with
-- Sales Reps-- this recursive CTE grabs the data and pairs each employee with their direct manager
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
            CASE WHEN LAYER = 1 THEN ID END AS LAYER_1_ID,
            CASE WHEN LAYER = 1 THEN NAME END AS LAYER_1_NAME,
            CASE WHEN LAYER = 2 THEN ID END AS LAYER_2_ID,
            CASE WHEN LAYER = 2 THEN NAME END AS LAYER_2_NAME,
            CASE WHEN LAYER = 3 THEN ID END AS LAYER_3_ID,
            CASE WHEN LAYER = 3 THEN NAME END AS LAYER_3_NAME,
            CASE WHEN LAYER = 4 THEN ID END AS LAYER_4_ID,
            CASE WHEN LAYER = 4 THEN NAME END AS LAYER_4_NAME,
            CASE WHEN LAYER = 5 THEN ID END AS LAYER_5_ID,
            CASE WHEN LAYER = 5 THEN NAME END AS LAYER_5_NAME,
            CASE WHEN LAYER = 6 THEN ID END AS LAYER_6_ID,
            CASE WHEN LAYER = 6 THEN NAME END AS LAYER_6_NAME
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
            COALESCE(
                shc.LAYER_1_ID,
                CASE WHEN sh.LAYER = 1 THEN sh.ID END
            ),
            COALESCE(
                shc.LAYER_1_NAME,
                CASE WHEN sh.LAYER = 1 THEN sh.NAME END
            ),
            COALESCE(
                shc.LAYER_2_ID,
                CASE WHEN sh.LAYER = 2 THEN sh.ID END
            ),
            COALESCE(
                shc.LAYER_2_NAME,
                CASE WHEN sh.LAYER = 2 THEN sh.NAME END
            ),
            COALESCE(
                shc.LAYER_3_ID,
                CASE WHEN sh.LAYER = 3 THEN sh.ID END
            ),
            COALESCE(
                shc.LAYER_3_NAME,
                CASE WHEN sh.LAYER = 3 THEN sh.NAME END
            ),
            COALESCE(
                shc.LAYER_4_ID,
                CASE WHEN sh.LAYER = 4 THEN sh.ID END
            ),
            COALESCE(
                shc.LAYER_4_NAME,
                CASE WHEN sh.LAYER = 4 THEN sh.NAME END
            ),
            COALESCE(
                shc.LAYER_5_ID,
                CASE WHEN sh.LAYER = 5 THEN sh.ID END
            ),
            COALESCE(
                shc.LAYER_5_NAME,
                CASE WHEN sh.LAYER = 5 THEN sh.NAME END
            ),
            COALESCE(
                shc.LAYER_6_ID,
                CASE WHEN sh.LAYER = 6 THEN sh.ID END
            ),
            COALESCE(
                shc.LAYER_6_NAME,
                CASE WHEN sh.LAYER = 6 THEN sh.NAME END
            )
        FROM {{ ref("stg_gsheets_hierarchy_table") }} sh
        JOIN
            hierarchy_cte shc
            ON sh.MANAGER_ID = shc.ID
            AND sh.START_DATE >= shc.START_DATE
            AND sh.END_DATE <= shc.END_DATE
    ),

    organization_table AS (
        SELECT
            MAX(LAYER_6_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS WATCHER_ID,
            MAX(LAYER_6_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS WATCHER_NAME,
            MAX(START_DATE) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS START_DATE,
            MAX(END_DATE) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS END_DATE,
            CASE
                WHEN
                    MAX(START_DATE) OVER (
                        PARTITION BY LAYER_6_ID, START_DATE, END_DATE
                    )
                    <= CURRENT_DATE
                    AND MAX(END_DATE) OVER (
                        PARTITION BY LAYER_6_ID, START_DATE, END_DATE
                    )
                    >= CURRENT_DATE
                THEN 1
                ELSE 0
            END AS CURRENT_RELATIONSHIP_FLAG,
            MAX(LAYER_5_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS SERGEANT_IN_COMMAND_ID,
            MAX(LAYER_5_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS SERGEANT_IN_COMMAND_NAME,
            MAX(LAYER_4_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS FIRST_CAPTAIN_ID,
            MAX(LAYER_4_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS FIRST_CAPTAIN_NAME,
            MAX(LAYER_3_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS BRIGHTCANDLE_ID,
            MAX(LAYER_3_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS BRIGHTCANDLE_NAME,
            MAX(LAYER_2_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS WISE_OWL_ID,
            MAX(LAYER_2_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS WISE_OWL_NAME,
            MAX(LAYER_1_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS HIGH_HARPER_ID,
            MAX(LAYER_1_ID) OVER (
                PARTITION BY LAYER_6_ID, START_DATE, END_DATE
            ) AS HIGH_HARPER_NAME
        FROM hierarchy_cte
        ORDER BY WATCHER_NAME ASC, START_DATE ASC
    )

select
    WATCHER_ID,
    -- this allows us to alter the titles in the underlying raw data without having to update this model
    WATCHER_NAME,
    START_DATE,
    END_DATE,
    CURRENT_RELATIONSHIP_FLAG,
    SERGEANT_IN_COMMAND_ID,
    SERGEANT_IN_COMMAND_NAME,
    FIRST_CAPTAIN_ID,
    FIRST_CAPTAIN_NAME,
    BRIGHTCANDLE_ID,
    BRIGHTCANDLE_NAME,
    WISE_OWL_ID,
    WISE_OWL_NAME,
    HIGH_HARPER_ID,
    HIGH_HARPER_NAME
FROM organization_table
WHERE WATCHER_ID IS NOT null
ORDER BY WATCHER_NAME ASC, START_DATE ASC