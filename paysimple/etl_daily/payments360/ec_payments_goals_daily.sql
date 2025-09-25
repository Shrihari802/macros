/*
This model expands monthly payment goals into daily goals by evenly distributing the monthly
goals across all days in the respective month.

It works by:
1. Generating a recursive calendar of all dates spanning from the earliest goal_month to the end of the last month.
2. Pulling monthly goal data from ec_payments_goals with relevant fields.
3. Calculating the number of days in each goal_month.
4. Joining the calendar to the monthly goals to create daily goals by dividing monthly goals by days in month.
5. Outputting daily-level goal data for each finance bucket and goal type.
*/

{{ config(materialized='table', dist="even") }}

WITH RECURSIVE all_dates(day) AS (
    -- Generate a calendar of all days covering the range of goal_months
    SELECT
        CAST(MIN(goal_month) AS DATE)                        AS day
    FROM {{ ref('ec_payments_goals') }}

    UNION ALL

    SELECT
        CAST(day + INTERVAL '1 day' AS DATE)
    FROM all_dates
    WHERE day + INTERVAL '1 day' <= (
        -- Calculate the last date as the last day of the last goal_month + 1 month - 1 day
        SELECT
            DATEADD(day, -1, DATEADD(month, 1, MAX(goal_month)))
        FROM {{ ref('ec_payments_goals') }}
    )
)

, monthly_goals AS (
    -- Select monthly goals and calculate the number of days in each month
    SELECT
          goal_month
        , unique_id
        , description
        , type
        , finance_bucket
        , goal_raw
        , goal_rounded
        , gateway
        , reforecast
        , budget
        , DATE_PART('day', DATEADD(day, -1, DATEADD(month, 1, goal_month))) AS days_in_month
    FROM {{ ref('ec_payments_goals') }}
)

, daily_goals AS (
    -- Distribute monthly goals evenly across each day in the month
    SELECT
          ad.day                                              AS period
        , mg.unique_id
        , mg.description
        , mg.type
        , mg.finance_bucket
        , mg.gateway
        , mg.reforecast
        , mg.budget
        , mg.goal_raw     / mg.days_in_month                  AS goal_raw   -- Daily prorated raw goal
        , mg.goal_rounded / mg.days_in_month                  AS goal_rounded  -- Daily prorated rounded goal
    FROM monthly_goals mg
    JOIN all_dates ad
      -- Join calendar dates to monthly goals where the date falls within the goal month
      ON ad.day BETWEEN mg.goal_month AND DATEADD(day, -1, DATEADD(month, 1, mg.goal_month))
)

SELECT
      period::DATE
    , unique_id
    , description
    , type
    , LOWER(finance_bucket)                                   AS finance_bucket  -- Normalize finance_bucket to lowercase
    , goal_raw
    , goal_rounded
    , gateway
    , reforecast
    , budget
FROM daily_goals
ORDER BY unique_id, period
