/*
This model calculates the daily Estimated Revenue by applying the target take rate
(Estimated Revenue goal divided by Total Processing Volume goal) to the actual daily TPV (Total Processing Volume).

It works by:
1. Pulling daily goals for Estimated Revenue and Total Processing Volume from the daily goals model.
2. Calculating the daily target take rate per solution as Estimated Revenue goal / Total Processing Volume goal.
3. Joining with actual daily TPV data.
4. Multiplying the TPV by the target take rate to estimate daily revenue.
5. Handling missing take rates by returning zero.
6. Labeling the metric as "Estimated Revenue".
*/

{{ config(materialized='table', dist="even") }}

WITH
-- Extract relevant daily goals (Estimated Revenue and Total Processing Volume)
daily_goals AS (
    SELECT
        CAST(period AS DATE)                  AS goal_date
      , LOWER(TRIM(finance_bucket))          AS solution
      , type
      , goal_raw
    FROM {{ ref('ec_payments_goals_daily') }}
    WHERE type IN ('Estimated Revenue', 'Total Processing Volume')
),

-- Calculate the target take rate: Estimated Revenue goal / Total Processing Volume goal per solution and date
daily_take_rate AS (
    SELECT
        goal_date
      , solution
      , CASE
            WHEN SUM(CASE WHEN type = 'Total Processing Volume' THEN goal_raw ELSE 0 END) = 0 THEN NULL
            ELSE
                SUM(CASE WHEN type = 'Estimated Revenue' THEN goal_raw ELSE 0 END) /
                SUM(CASE WHEN type = 'Total Processing Volume' THEN goal_raw ELSE 0 END)
        END                              AS target_take_rate
    FROM daily_goals
    GROUP BY goal_date, solution
),

-- Get actual daily Total Processing Volume data per solution
daily_tpv AS (
    SELECT
        LOWER(TRIM(solution))                 AS solution
      , CAST(period AS DATE)                  AS period
      , value                               AS tpv
    FROM {{ ref('total_processing_volume_daily') }}
),

-- Calculate estimated revenue as TPV * target take rate for each day and solution
estimated_revenue AS (
    SELECT
        'Estimated Revenue'                  AS metric_key
      , t.solution
      , t.period
      , 'day'                              AS period_grain
      , CASE
            WHEN tr.target_take_rate IS NULL THEN 0
            ELSE t.tpv * tr.target_take_rate
        END                                AS value
    FROM daily_tpv t
    LEFT JOIN daily_take_rate tr
      ON t.solution = tr.solution
     AND t.period = tr.goal_date
)

-- Final output with properly formatted columns
SELECT
      metric_key
    , solution
    , period::DATE
    , period_grain
    , CAST(ROUND(value, 2) AS NUMERIC(20,2))   AS value
FROM estimated_revenue
ORDER BY period, solution
