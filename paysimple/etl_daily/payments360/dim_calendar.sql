{{ config(materialized='table', dist='even') }}

WITH
    RECURSIVE date_spine ( date_key ) AS (
                                         SELECT
                                             DATEADD( YEAR, -10, CURRENT_DATE )::DATE
                                         UNION ALL
                                         SELECT
                                             DATEADD( DAY, 1, date_key )::DATE
                                         FROM date_spine
                                         WHERE
                                             date_key < DATEADD( YEAR, 10, CURRENT_DATE )::DATE
                                         )
  ,           current_context         AS (
                                         SELECT
                                             CURRENT_DATE::DATE                                              AS today
                                           , DATE_TRUNC( 'week', CURRENT_DATE )::DATE                        AS current_week_start
                                           , DATE_TRUNC( 'month', CURRENT_DATE )::DATE                       AS current_month_start
                                           , DATE_TRUNC( 'quarter', CURRENT_DATE )::DATE                     AS current_quarter_start
                                           , DATE_TRUNC( 'year', CURRENT_DATE )::DATE                        AS current_year_start
                                           , DATE_TRUNC( 'month', DATEADD( MONTH, -1, CURRENT_DATE ) )::DATE AS previous_month_start
                                           , DATE_TRUNC( 'year', DATEADD( YEAR, -1, CURRENT_DATE ) )::DATE   AS previous_year_start
                                         )

SELECT
    ds.date_key
  , EXTRACT( YEAR FROM ds.date_key )                                              AS year
  , EXTRACT( QUARTER FROM ds.date_key )                                           AS quarter
  , EXTRACT( MONTH FROM ds.date_key )                                             AS month
  , EXTRACT( WEEK FROM ds.date_key )                                              AS week_of_year
  , EXTRACT( DAY FROM ds.date_key )                                               AS day_of_month
  , TO_CHAR( ds.date_key, 'Month' )                                               AS month_name
  , TO_CHAR( ds.date_key, 'YYYY-MM' )                                             AS year_month
  , TO_CHAR( ds.date_key, 'YYYY' ) || '-Q' || EXTRACT( QUARTER FROM ds.date_key ) AS year_quarter
  , TO_CHAR( ds.date_key, 'YYYY' ) || '-W' ||
    LPAD( EXTRACT( WEEK FROM ds.date_key )::VARCHAR, 2, '0' )                     AS year_week

    -- Period starts
  , DATE_TRUNC( 'month', ds.date_key )                                            AS month_start
  , DATE_TRUNC( 'quarter', ds.date_key )                                          AS quarter_start
  , DATE_TRUNC( 'year', ds.date_key )                                             AS year_start

    -- Period lengths
  , DATEDIFF( DAY, DATE_TRUNC( 'month', ds.date_key ),
              DATEADD( MONTH, 1, DATE_TRUNC( 'month', ds.date_key ) ) )           AS days_in_month
  , DATEDIFF( DAY, DATE_TRUNC( 'quarter', ds.date_key ),
              DATEADD( QUARTER, 1, DATE_TRUNC( 'quarter', ds.date_key ) ) )       AS days_in_quarter
  , DATEDIFF( DAY, DATE_TRUNC( 'year', ds.date_key ),
              DATEADD( YEAR, 1, DATE_TRUNC( 'year', ds.date_key ) ) )             AS days_in_year

    -- Completion for EVERY day within its own period
  , ROUND( EXTRACT( DAY FROM ds.date_key )::DECIMAL
               / NULLIF( DATEDIFF( DAY, DATE_TRUNC( 'month', ds.date_key ),
                                   DATEADD( MONTH, 1, DATE_TRUNC( 'month', ds.date_key ) ) ), 0 ), 2 )
                                                                                  AS month_completion_pct
  , ROUND( (DATEDIFF( DAY, DATE_TRUNC( 'quarter', ds.date_key ), ds.date_key ) + 1)::DECIMAL
               / NULLIF( DATEDIFF( DAY, DATE_TRUNC( 'quarter', ds.date_key ),
                                   DATEADD( QUARTER, 1, DATE_TRUNC( 'quarter', ds.date_key ) ) ), 0 ), 2 )
                                                                                  AS quarter_completion_pct
  , ROUND( EXTRACT( DOY FROM ds.date_key )::DECIMAL
               / NULLIF( DATEDIFF( DAY, DATE_TRUNC( 'year', ds.date_key ),
                                   DATEADD( YEAR, 1, DATE_TRUNC( 'year', ds.date_key ) ) ), 0 ), 2 )
                                                                                  AS year_completion_pct

    -- Current-period flags
  , (ds.date_key = cc.today)                                                      AS is_current_day
  , (ds.date_key <= cc.today)                                                     AS is_in_past_or_today
  , (DATE_TRUNC( 'week', ds.date_key ) = cc.current_week_start)                   AS is_current_week
  , (DATE_TRUNC( 'month', ds.date_key ) = cc.current_month_start)                 AS is_current_month
  , (DATE_TRUNC( 'quarter', ds.date_key ) = cc.current_quarter_start)             AS is_current_quarter
  , (DATE_TRUNC( 'year', ds.date_key ) = cc.current_year_start)                   AS is_current_year
  , (DATE_TRUNC( 'month', ds.date_key ) = cc.previous_month_start)                AS is_previous_month
  , (DATE_TRUNC( 'year', ds.date_key ) = cc.previous_year_start)                  AS is_previous_year

    -- Periods ago vs today
  , CASE
        WHEN DATE_TRUNC( 'month', ds.date_key ) = cc.current_month_start
            THEN 0
        WHEN DATE_TRUNC( 'month', ds.date_key ) = cc.previous_month_start
            THEN 1
        WHEN ds.date_key < cc.current_month_start
            THEN DATEDIFF( MONTH, DATE_TRUNC( 'month', ds.date_key ), cc.current_month_start )
            ELSE NULL
    END                                                                           AS mtd_periods_ago
  , CASE
        WHEN DATE_TRUNC( 'year', ds.date_key ) = cc.current_year_start
            THEN 0
        WHEN DATE_TRUNC( 'year', ds.date_key ) = cc.previous_year_start
            THEN 1
        WHEN ds.date_key < cc.current_year_start
            THEN DATEDIFF( YEAR, DATE_TRUNC( 'year', ds.date_key ), cc.current_year_start )
            ELSE NULL
    END                                                                           AS ytd_periods_ago

FROM date_spine ds
CROSS JOIN current_context cc
ORDER BY ds.date_key
