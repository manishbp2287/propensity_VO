import numpy as np
import pandas as pd
import pandas_gbq


def push_merchant_in_application_7d(task_id = (16, -1),order_date = "order_date",table = "bharatpe_analytics_data.bharatswipe_application", status =('ORDERED', 'ACTIVE')):
    merchant_push_query = f"""
            WITH
              swipe_order AS (
            SELECT
              merchant_id,
              MIN(DATE({order_date})) AS order_date
            FROM
              `bharatpe-analytics-prod.{table}`
            WHERE
              DATE(created_at) >= "2018-01-01"
            and STATUS IN {status}
            GROUP BY
              merchant_id
             ),

             push_temp as(

             (
              SELECT
                distinct case when DATE(h.created_at) <= so.order_date and DATE(h.created_at) > DATE_SUB(Date(so.order_date), INTERVAL 7 DAY) then h.merchant_id else null end as merchant_id
                
              FROM
                `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history` h
                join swipe_order so
                on h.merchant_id = so.merchant_id
              WHERE
                task_id in {task_id}
                )
            UNION distinct (
              SELECT
                distinct case when DATE(h2.created_at) <= so.order_date and DATE(h2.created_at) > DATE_SUB(Date(so.order_date), INTERVAL 7 DAY) then h2.merchant_id else null end as merchant_id
                
              FROM
                `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history_v2` h2
                    join swipe_order so
                on h2.merchant_id = so.merchant_id
              WHERE
                task_id in {task_id}
                )
             )

             select distinct merchant_id from push_temp
             where merchant_id is not null
    """
    temp = pandas_gbq.read_gbq(merchant_push_query)
    return temp


def total_push_merchants(task_id = (16, -1)):
    total_push_query = f"""
            (
          SELECT
            distinct merchant_id
          FROM
            `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history`
          WHERE
            DATE(created_at) >= "2018-01-01"
            AND task_id in {task_id}
            )
        UNION distinct (
          SELECT
            distinct merchant_id
          FROM
            `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history_v2`
          WHERE
            DATE(created_at) >= "2018-01-01"
            AND task_id in {task_id}
            )
    """
    temp = pandas_gbq.read_gbq(total_push_query)
    return temp


def total_applications(table = "bharatpe_analytics_data.bharatswipe_application", status =('ORDERED', 'ACTIVE')):
    total_applications_query = f"""
            SELECT
              distinct merchant_id
            FROM
              `bharatpe-analytics-prod.{table}`
            WHERE
              DATE(created_at) >= "2018-01-01"
            and STATUS IN {status}
    """
    temp = pandas_gbq.read_gbq(total_applications_query)
    return temp


def merchant_data(partitions = 10, task_id = (16, -1), table ='bharatpe_analytics_data.bharatswipe_application', status =('ORDERED', 'ACTIVE')):
    partitions = partitions
    d = {}
    for i in range(0, partitions):
        merchant_data_mart_query = f"""
        WITH
          push_merch AS ( (
            SELECT
              DISTINCT merchant_id
            FROM
              `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history`
            WHERE
              DATE(created_at) >= "2018-01-01"
              AND task_id in {task_id}
              )
          UNION distinct (
            SELECT
              DISTINCT merchant_id
            FROM
              `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history_v2`
            WHERE
              DATE(created_at) >= "2018-01-01"
              AND task_id in {task_id}
              ) ),
          app_merch AS (
          SELECT
            DISTINCT merchant_id
          FROM
            `bharatpe-analytics-prod.{table}`
          WHERE
            DATE(created_at) >= "2018-01-01"
            AND STATUS IN {status} ),
          final_merch AS (
          SELECT
            *
          FROM
            app_merch
          UNION distinct
          SELECT
            *
          FROM
            push_merch)
        SELECT
          merchant_id,
          mid,
          actual_merchant_type,
          area_name,
          zone_name,
          region_name,
          territory_name,
          merchant_created_at,
          std_category as business_category,
          st_geohash(st_geogpoint(integ_longitude, integ_latitude),6) as geohash6,
          st_geohash(st_geogpoint(integ_longitude, integ_latitude),3) as geohash3,
          st_geohash(st_geogpoint(integ_longitude, integ_latitude),4) as geohash4,
          swipe_flag,
          speaker_flag,
          loan_flag,
          fp_flag,
          club_flag,
          qr_flag,
          credit_flag,
          debit_flag,
          actual_merchant_type as merchant_acquisition_channel,
          onboarding_date,
          lending_eligibility,
          lending_eligibility_type,
          swipe_eligibility,
          fp_eligibility,
          insurance_eligibility,
          club_eligibility,
          gl_eligibility,
          speaker_eligibility,
          merchant_score,
          enagagement_score,
          loan_disbursal_date,
          loan_closure_date,
          swipe_order_date,
          speaker_order_date,
          club_start_date,
          club_end_date
        FROM
          `bharatpe-analytics-prod.bharatpe_data_platfrom.universal_merchant_data_mart`
        WHERE
           merchant_id in (
           select distinct merchant_id from final_merch) 
          AND DATE(merchant_created_at) >= "2018-01-01"
          AND MOD(merchant_id, {partitions})={i}
        ORDER BY
        merchant_id
        """
        d[i] = pandas_gbq.read_gbq(merchant_data_mart_query)
    temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
    return temp


def trx_data(partitions = 10, task_id = (16, -1),order_date = "order_date", table = "bharatpe_analytics_data.bharatswipe_application", status =('ORDERED', 'ACTIVE')):
    partitions = partitions
    d = {}
    for i in range(0, partitions):
        trx_query = f"""
 WITH
  push_merch AS ( (
    SELECT
      merchant_id,
      MIN(DATE(created_at)) AS order_date
    FROM
      `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history`
    WHERE
      DATE(created_at) >= "2018-01-01"
      AND task_id IN {task_id}
    GROUP BY
      1)
  UNION DISTINCT (
    SELECT
      merchant_id,
      MIN(DATE(created_at)) AS order_date
    FROM
      `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history_v2`
    WHERE
      DATE(created_at) >= "2018-01-01"
      AND task_id IN {task_id}
    GROUP BY
      1) ),
  app_merch AS (
  SELECT
    DISTINCT merchant_id
  FROM
    `bharatpe-analytics-prod.{table}`
  WHERE
    DATE(created_at) >= "2018-01-01"
    AND STATUS IN {status} ),
  final_merch AS (
  SELECT
    *
  FROM
    app_merch
  UNION DISTINCT
  SELECT
    DISTINCT merchant_id
  FROM
    push_merch),
  merchant_list AS (
  SELECT
    DISTINCT merchant_id
  FROM
    final_merch),
  swipe_order1 AS (
  SELECT
    merchant_id,
    MIN(DATE({order_date})) AS order_date
  FROM
    `bharatpe-analytics-prod.{table}`
  WHERE
    merchant_id IN (
    SELECT
      *
    FROM
      merchant_list)
  GROUP BY
    merchant_id),
  swipe_order2 AS (
  SELECT
    COALESCE(so1.merchant_id, pm.merchant_id) AS merchant_id,
    COALESCE(so1.order_date, pm.order_date) AS order_date
  FROM
    push_merch pm
  FULL OUTER JOIN
    swipe_order1 AS so1
  ON
    so1.merchant_id = pm.merchant_id ),
  swipe_order AS (
  SELECT
    merchant_id,
    order_date
  FROM
    swipe_order2 ),
  txn_temp AS (
  SELECT
    id,
    merchant_id,
    amount,
    DATE(payment_timestamp) AS payment_date,
    payer_vpa_hash AS user
  FROM
    `bharatpe-analytics-prod.payin.transactions`
  WHERE
    status = "SUCCESS"
    AND merchant_id IN (
    SELECT
      *
    FROM
      merchant_list )),
  txn_temp2 AS (
  SELECT
    merchant_id,
    DATE(payment_timestamp) AS payment_date,
    SUM(amount) AS daily_tpv,
    COUNT(DISTINCT payer_vpa_hash) AS daily_payees,
    COUNT(DISTINCT id) AS daily_txns
  FROM
    `bharatpe-analytics-prod.payin.transactions`
  WHERE
    status = "SUCCESS"
    AND merchant_id IN (
    SELECT
      *
    FROM
      merchant_list )
  GROUP BY
    1,
    2 ),
  txn1 AS (
  SELECT
    tt.merchant_id,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 7 DAY) THEN tt.user
      ELSE
      NULL
    END
      ) AS w1_unique_payers,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 7 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 14 DAY) THEN tt.user
      ELSE
      NULL
    END
      ) AS w2_unique_payers,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) THEN tt.user
      ELSE
      NULL
    END
      ) AS m1_unique_payers,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY)AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) THEN tt.user
      ELSE
      NULL
    END
      ) AS m2_unique_payers,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY)AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 90 DAY) THEN tt.user
      ELSE
      NULL
    END
      ) AS m3_unique_payers,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 90 DAY) THEN tt.user
      ELSE
      NULL
    END
      ) AS unique_payers_90_days,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < so.order_date THEN tt.user
      ELSE
      NULL
    END
      ) AS total_unique_payers,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 7 DAY) THEN tt.id
      ELSE
      NULL
    END
      ) AS w1_txn_count,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 7 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 14 DAY) THEN tt.id
      ELSE
      NULL
    END
      ) AS w2_txn_count,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 14 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 21 DAY) THEN tt.id
      ELSE
      NULL
    END
      ) AS w3_txn_count,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) THEN tt.id
      ELSE
      NULL
    END
      ) AS m1_txn_count,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY)AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) THEN tt.id
      ELSE
      NULL
    END
      ) AS m2_txn_count,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY)AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 90 DAY) THEN tt.id
      ELSE
      NULL
    END
      ) AS m3_txn_count,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 90 DAY) THEN tt.id
      ELSE
      NULL
    END
      ) AS txn_count_90_days,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < so.order_date THEN tt.id
      ELSE
      NULL
    END
      ) AS total_txn_count,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < so.order_date THEN DATE(tt.payment_date)
      ELSE
      NULL
    END
      ) AS total_active_vintage,
    SUM(CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 7 DAY) THEN tt.amount
      ELSE
      0
    END
      ) AS w1_tpv,
    SUM(CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 7 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 14 DAY) THEN tt.amount
      ELSE
      0
    END
      ) AS w2_tpv,
    SUM(CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) THEN tt.amount
      ELSE
      0
    END
      ) AS m1_tpv,
    SUM(CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) THEN tt.amount
      ELSE
      0
    END
      ) AS m2_tpv,
    SUM(CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 90 DAY) THEN tt.amount
      ELSE
      0
    END
      ) AS m3_tpv,
    SUM(CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 90 DAY) THEN tt.amount
      ELSE
      0
    END
      ) AS tpv_90_days,
    SUM(CASE
        WHEN tt.payment_date < so.order_date THEN tt.amount
      ELSE
      0
    END
      ) AS total_tpv,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) THEN EXTRACT(DAY FROM DATE(tt.payment_date))
      ELSE
      NULL
    END
      ) AS m1_active_days,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) THEN EXTRACT(DAY FROM DATE(tt.payment_date))
      ELSE
      NULL
    END
      ) AS m2_active_days,
    COUNT(DISTINCT
      CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 90 DAY) THEN EXTRACT(DAY FROM DATE(tt.payment_date))
      ELSE
      NULL
    END
      ) AS m3_active_days,
    MAX(CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) THEN tt2.daily_tpv
      ELSE
      0
    END
      ) AS m1_max_daily_tpv,
    MAX(CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) THEN tt2.daily_tpv
      ELSE
      0
    END
      ) AS m2_max_daily_tpv,
    MAX(CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 90 DAY) THEN tt2.daily_tpv
      ELSE
      0
    END
      ) AS m3_max_daily_tpv,
    MAX(CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) THEN tt2.daily_payees
      ELSE
      0
    END
      ) AS m1_max_daily_payees,
    MAX(CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) THEN tt2.daily_payees
      ELSE
      0
    END
      ) AS m2_max_daily_payees,
    MAX(CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 90 DAY) THEN tt2.daily_payees
      ELSE
      0
    END
      ) AS m3_max_daily_payees,
    MAX(CASE
        WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) THEN tt2.daily_txns
      ELSE
      0
    END
      ) AS m1_max_daily_txns,
    MAX(CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 30 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) THEN tt2.daily_txns
      ELSE
      0
    END
      ) AS m2_max_daily_txns,
    MAX(CASE
        WHEN tt.payment_date < DATE_SUB(DATE(so.order_date), INTERVAL 60 DAY) AND tt.payment_date >= DATE_SUB(DATE(so.order_date), INTERVAL 90 DAY) THEN tt2.daily_txns
      ELSE
      0
    END
      ) AS m3_max_daily_txns
  FROM
    txn_temp tt
  JOIN
    swipe_order so
  ON
    tt.merchant_id = so.merchant_id
  JOIN
    txn_temp2 tt2
  ON
    tt.merchant_id = tt2.merchant_id
    AND tt.payment_date = tt2.payment_date
  GROUP BY
    1 )
SELECT
  *
FROM
  txn1
WHERE
  MOD(merchant_id, {partitions})={i}
        """
        d[i] = pandas_gbq.read_gbq(trx_query)
    temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
    return temp


def app_sessions(partitions = 10, start_date = "2020-01-01", end_date ="2021-01-01",task_id = (16, -1),order_date = "order_date", table = "bharatpe_analytics_data.bharatswipe_application", events = ('swipe_home_screen_landed','swipe_od_screen_landed','swipe_home_button_click_order_now','swipe_od_button_click_submit','bharat_swipe'),status =('ORDERED', 'ACTIVE')):
    partitions = partitions
    d = {}
    for i in range(0, partitions):
        app_query = f"""
          WITH
  push_merch AS ( (
    SELECT
      merchant_id,
      MIN(DATE(created_at)) AS order_date
    FROM
      `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history`
    WHERE
      DATE(created_at) >= "2018-01-01"
      AND task_id IN {task_id}
    GROUP BY
      1)
  UNION DISTINCT (
    SELECT
      merchant_id,
      MIN(DATE(created_at)) AS order_date
    FROM
      `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history_v2`
    WHERE
      DATE(created_at) >= "2018-01-01"
      AND task_id IN {task_id}
    GROUP BY
      1) ),
  app_merch AS (
  SELECT
    DISTINCT merchant_id
  FROM
    `bharatpe-analytics-prod.{table}`
  WHERE
    DATE(created_at) >= "2018-01-01"
    AND STATUS IN {status} ),
  final_merch AS (
  SELECT
    *
  FROM
    app_merch
  UNION DISTINCT
  SELECT
    DISTINCT merchant_id
  FROM
    push_merch),
  merchant_list AS (
  SELECT
    dm.merchant_id AS merchant_id,
    dm.mid AS mid
  FROM
    final_merch bs
  JOIN
    `bharatpe-analytics-prod.bharatpe_data_platfrom.universal_merchant_data_mart` dm
  ON
    bs.merchant_id = dm.merchant_id
  WHERE
    DATE(dm.merchant_created_at) >= "{start_date}"
    AND DATE(dm.merchant_created_at) < "{end_date}"),
  swipe_order1 AS (
  SELECT
    merchant_id,
    MIN(DATE({order_date})) AS order_date
  FROM
    `bharatpe-analytics-prod.{table}`
  WHERE
    merchant_id IN (
    SELECT
      merchant_id
    FROM
      merchant_list)
  GROUP BY
    merchant_id),
  swipe_order2 AS (
  SELECT
    COALESCE(so1.merchant_id, pm.merchant_id) AS merchant_id,
    COALESCE(so1.order_date, pm.order_date) AS order_date
  FROM
    push_merch pm
  FULL OUTER JOIN
    swipe_order1 AS so1
  ON
    so1.merchant_id = pm.merchant_id ),
  swipe_order AS (
  SELECT
    merchant_id,
    order_date
  FROM
    swipe_order2 ),  
          swipe_temp AS(
          SELECT
            merchant_list.merchant_id,
            mid,
            order_date
          FROM
            merchant_list
          JOIN
            swipe_order
          ON
            merchant_list.merchant_id = swipe_order.merchant_id )
        SELECT
          st.merchant_id,
          COUNT(DISTINCT
            CASE
              WHEN DATE(created_at)< st.order_date AND DATE(created_at) >= DATE_SUB(DATE(st.order_date), INTERVAL 7 DAY) THEN session_id
            ELSE
            NULL
          END
            ) AS w1_sessions,
          COUNT(DISTINCT
            CASE
              WHEN DATE(created_at)< st.order_date AND DATE(created_at) >= DATE_SUB(DATE(st.order_date), INTERVAL 30 DAY) THEN session_id
            ELSE
            NULL
          END
            ) AS m1_sessions
        FROM
          `bharatpe-analytics-prod.bharatpe_analytics_data.app_event` ae
        JOIN
          swipe_temp st
        ON
          ae.mid = st.mid
        WHERE
          created_at >= "{start_date}"
          AND created_at < "{end_date}"
          AND session_id IS NOT NULL
          AND SOURCE = "MerchantApp"
          AND event_name IN {events}
          AND MOD(merchant_id, {partitions})={i}
        GROUP BY
          1
        HAVING
          w1_sessions > 0
        """
        d[i] = pandas_gbq.read_gbq(app_query)
    temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
    return temp


def tot_vintage(partitions = 10, task_id = (16, -1),order_date = "order_date", table = "bharatpe_analytics_data.bharatswipe_application", status =('ORDERED', 'ACTIVE')):
    partitions = partitions
    d = {}
    for i in range(0, partitions):
        app_query = f"""
        WITH
          push_merch AS ( (
            SELECT
              merchant_id,
              MIN(DATE(created_at)) AS order_date
            FROM
              `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history`
            WHERE
              DATE(created_at) >= "2018-01-01"
              AND task_id IN {task_id}
            GROUP BY
              1)
          UNION DISTINCT (
            SELECT
              merchant_id,
              MIN(DATE(created_at)) AS order_date
            FROM
              `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history_v2`
            WHERE
              DATE(created_at) >= "2018-01-01"
              AND task_id IN {task_id}
            GROUP BY
              1) ),

          swipe_order1 AS (
          SELECT
            merchant_id,
            MIN(DATE({order_date})) AS order_date
          FROM
            `bharatpe-analytics-prod.{table}`
            where status in {status}

          GROUP BY
            merchant_id),

          swipe_order2 AS (
          SELECT
            COALESCE(so1.merchant_id, pm.merchant_id) AS merchant_id,
            COALESCE(so1.order_date, pm.order_date) AS order_date
          FROM
            push_merch pm
          FULL OUTER JOIN
            swipe_order1 AS so1
          ON
            so1.merchant_id = pm.merchant_id ),
          swipe_order AS (
          SELECT
            merchant_id,
            order_date
          FROM
            swipe_order2 )


        SELECT
          dm.merchant_id,
          DATE_DIFF(so.order_date, date(merchant_created_at), DAY) as vintage
        FROM
          `bharatpe-analytics-prod.bharatpe_data_platfrom.universal_merchant_data_mart` dm
        join swipe_order so
        on so.merchant_id = dm.merchant_id
        WHERE
          DATE(merchant_created_at) >= "2018-01-01"
          and dm.merchant_id in (
            select merchant_id from swipe_order

        )

  and MOD(dm.merchant_id, {partitions})={i}
        """
        d[i] = pandas_gbq.read_gbq(app_query)
    temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
    return temp


# def trx_data(partitions = 10, task_id = (16, -1),order_date = "order_date", table = "bharatpe_analytics_data.bharatswipe_application", status =('ORDERED', 'ACTIVE')):
#     partitions = partitions
#     d = {}
#     for i in range(0, partitions):
#         trx_query = f"""
#         WITH push_merch AS ( (
#             SELECT
#               DISTINCT merchant_id
#             FROM
#               `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history`
#             WHERE
#               DATE(created_at) >= "2018-01-01"
#               AND task_id in {task_id}
#               )
#           UNION distinct (
#             SELECT
#               DISTINCT merchant_id
#             FROM
#               `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history_v2`
#             WHERE
#               DATE(created_at) >= "2018-01-01"
#               AND task_id in {task_id}
#               ) ),
#           app_merch AS (
#           SELECT
#             DISTINCT merchant_id
#           FROM
#             `bharatpe-analytics-prod.{table}`
#           WHERE
#             DATE(created_at) >= "2018-01-01"
#             AND STATUS IN {status} ),
#           final_merch AS (
#           SELECT
#             *
#           FROM
#             app_merch
#           UNION distinct
#           SELECT
#             *
#           FROM
#             push_merch),
        
#         merchant_list AS (
#             select distinct merchant_id
#             from final_merch),

#           swipe_order AS (
#           SELECT
#             merchant_id,
#             MIN(DATE({order_date})) AS order_date
#           FROM
#             `bharatpe-analytics-prod.{table}`
#           WHERE
#             merchant_id IN (
#             select * from merchant_list)
#           GROUP BY
#             merchant_id),

#           txn_temp AS (
#           SELECT
#             id,
#             merchant_id,
#             amount,
#             DATE(payment_timestamp) AS payment_date,
#             payer_vpa_hash AS user
#           FROM
#             `bharatpe-analytics-prod.payin.transactions`
#           WHERE
#             status = "SUCCESS"
#             AND merchant_id IN (
#               select * from merchant_list
#             )),

#             txn_temp2 AS (
#           SELECT
#             merchant_id,
#             DATE(payment_timestamp) AS payment_date,
#             sum(amount) as daily_tpv,
#             COUNT(DISTINCT payer_vpa_hash) AS daily_payees,
#             COUNT(DISTINCT id) AS daily_txns
#           FROM
#             `bharatpe-analytics-prod.payin.transactions`
#           WHERE
#             status = "SUCCESS"
#             AND merchant_id IN (
#               select * from merchant_list
#             )
#             group by 1,2
#             ),

#           txn1 AS (
#           SELECT tt.merchant_id,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 7 DAY) THEN tt.user else Null end) AS w1_unique_payers,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 7 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 14 DAY) THEN tt.user else Null end) AS w2_unique_payers,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) THEN tt.user else Null end) AS m1_unique_payers,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 30 DAY)AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) THEN tt.user else Null end) AS m2_unique_payers,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 60 DAY)AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 90 DAY) THEN tt.user else Null end) AS m3_unique_payers,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 90 DAY) THEN tt.user else Null end) AS unique_payers_90_days,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < so.order_date THEN tt.user else Null end) AS total_unique_payers,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 7 DAY) THEN tt.id else Null end) AS w1_txn_count,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 7 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 14 DAY) THEN tt.id else Null end) AS w2_txn_count,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 14 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 21 DAY) THEN tt.id else Null end) AS w3_txn_count,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) THEN tt.id else Null end) AS m1_txn_count,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 30 DAY)AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) THEN tt.id else Null end) AS m2_txn_count,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 60 DAY)AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 90 DAY) THEN tt.id else Null end) AS m3_txn_count,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 90 DAY) THEN tt.id else Null end) AS txn_count_90_days,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < so.order_date THEN tt.id else Null end) AS total_txn_count,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < so.order_date THEN DATE(tt.payment_date) else Null end) AS total_active_vintage,
#           sum(CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 7 DAY) THEN tt.amount else 0 end) AS w1_tpv,
#           sum(CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 7 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 14 DAY) THEN tt.amount else 0 end) AS w2_tpv,
#           sum(CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) THEN tt.amount else 0 end) AS m1_tpv,
#           sum(CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) THEN tt.amount else 0 end) AS m2_tpv,
#           sum(CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 90 DAY) THEN tt.amount else 0 end) AS m3_tpv,
#           sum(CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 90 DAY) THEN tt.amount else 0 end) AS tpv_90_days,
#           sum(CASE WHEN tt.payment_date < so.order_date THEN tt.amount else 0 end) AS total_tpv,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) THEN DATE(tt.payment_date) else Null end) AS m1_active_days,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) THEN DATE(tt.payment_date) else Null end) AS m2_active_days,
#           COUNT(DISTINCT CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 90 DAY) THEN DATE(tt.payment_date) else Null end) AS m3_active_days,
#           max(CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) THEN tt2.daily_tpv else 0 end) AS m1_max_daily_tpv,
#           max(CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) THEN tt2.daily_tpv else 0 end) AS m2_max_daily_tpv,
#           max(CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 90 DAY) THEN tt2.daily_tpv else 0 end) AS m3_max_daily_tpv,
#           max(CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) THEN tt2.daily_payees else 0 end) AS m1_max_daily_payees,
#           max(CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) THEN tt2.daily_payees else 0 end) AS m2_max_daily_payees,
#           max(CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 90 DAY) THEN tt2.daily_payees else 0 end) AS m3_max_daily_payees,
#           max(CASE WHEN tt.payment_date < so.order_date AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) THEN tt2.daily_txns else 0 end) AS m1_max_daily_txns,
#           max(CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 30 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) THEN tt2.daily_txns else 0 end) AS m2_max_daily_txns,
#           max(CASE WHEN tt.payment_date < DATE_SUB(Date(so.order_date), INTERVAL 60 DAY) AND tt.payment_date >= DATE_SUB(Date(so.order_date), INTERVAL 90 DAY) THEN tt2.daily_txns else 0 end) AS m3_max_daily_txns

#           FROM txn_temp tt
#           JOIN swipe_order so
#           ON tt.merchant_id = so.merchant_id
#           JOIN txn_temp2 tt2
#           ON tt.merchant_id = tt2.merchant_id
#           AND tt.payment_date = tt2.payment_date
#           GROUP BY 1
#         )

#         select * from txn1
#         where MOD(merchant_id, {partitions})={i}
#         """
#         d[i] = pandas_gbq.read_gbq(trx_query)
#     temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
#     return temp



# def app_sessions(partitions = 10, start_date = "2020-01-01", end_date ="2021-01-01",task_id = (16, -1),order_date = "order_date", table = "bharatpe_analytics_data.bharatswipe_application", events = ('swipe_home_screen_landed','swipe_od_screen_landed','swipe_home_button_click_order_now','swipe_od_button_click_submit','bharat_swipe'),status =('ORDERED', 'ACTIVE')):
#     partitions = partitions
#     d = {}
#     for i in range(0, partitions):
#         app_query = f"""
#             WITH push_merch AS ( (
#             SELECT
#               DISTINCT merchant_id
#             FROM
#               `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history`
#             WHERE
#               DATE(created_at) >= "2018-01-01"
#               AND task_id in {task_id}
#               )
#           UNION distinct (
#             SELECT
#               DISTINCT merchant_id
#             FROM
#               `bharatpe-analytics-prod.bp_lastmile.lmp_user_task_history_v2`
#             WHERE
#               DATE(created_at) >= "2018-01-01"
#               AND task_id in {task_id}
#               ) ),
#           app_merch AS (
#           SELECT
#             DISTINCT merchant_id
#           FROM
#             `bharatpe-analytics-prod.{table}`
#           WHERE
#             DATE(created_at) >= "2018-01-01"
#             AND STATUS IN {status} ),
#           final_merch AS (
#           SELECT
#             *
#           FROM
#             app_merch
#           UNION distinct
#           SELECT
#             *
#           FROM
#             push_merch),
        
#         merchant_list AS (
#           SELECT
#             dm.merchant_id AS merchant_id,
#             dm.mid AS mid
#           FROM
#             final_merch bs
#           JOIN
#             `bharatpe-analytics-prod.bharatpe_data_platfrom.universal_merchant_data_mart` dm
#           ON
#             bs.merchant_id = dm.merchant_id
#           WHERE
#             DATE(dm.merchant_created_at) >= "{start_date}"
#             AND DATE(dm.merchant_created_at) < "{end_date}"),
#           swipe_order AS (
#           SELECT
#             merchant_id,
#             MIN(DATE({order_date})) AS order_date
#           FROM
#             `bharatpe-analytics-prod.{table}`
#           WHERE
#             merchant_id IN (
#             SELECT
#               merchant_id
#             FROM
#               merchant_list)
#           GROUP BY
#             merchant_id),
#           swipe_temp AS(
#           SELECT
#             merchant_list.merchant_id,
#             mid,
#             order_date
#           FROM
#             merchant_list
#           JOIN
#             swipe_order
#           ON
#             merchant_list.merchant_id = swipe_order.merchant_id )
#         SELECT
#           st.merchant_id,
#           COUNT(DISTINCT
#             CASE
#               WHEN DATE(created_at)< st.order_date AND DATE(created_at) >= DATE_SUB(DATE(st.order_date), INTERVAL 7 DAY) THEN session_id
#             ELSE
#             NULL
#           END
#             ) AS w1_sessions,
#           COUNT(DISTINCT
#             CASE
#               WHEN DATE(created_at)< st.order_date AND DATE(created_at) >= DATE_SUB(DATE(st.order_date), INTERVAL 30 DAY) THEN session_id
#             ELSE
#             NULL
#           END
#             ) AS m1_sessions
#         FROM
#           `bharatpe-analytics-prod.bharatpe_analytics_data.app_event` ae
#         JOIN
#           swipe_temp st
#         ON
#           ae.mid = st.mid
#         WHERE
#           created_at >= "{start_date}"
#           AND created_at < "{end_date}"
#           AND session_id IS NOT NULL
#           AND SOURCE = "MerchantApp"
#           AND event_name IN {events}
#           AND MOD(merchant_id, {partitions})={i}
#         GROUP BY
#           1
#         HAVING
#           w1_sessions > 0
#         """
#         d[i] = pandas_gbq.read_gbq(app_query)
#     temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
#     return temp