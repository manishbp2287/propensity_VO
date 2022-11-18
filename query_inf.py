import numpy as np
import pandas as pd
import pandas_gbq


def merchant_data(partitions = 10):
    partitions = partitions
    d = {}
    for i in range(0, partitions):
        merchant_data_mart_query = f"""
    SELECT
      merchant_id,
      mid,
      actual_merchant_type,
      DATE_DIFF(CURRENT_DATE(), date(merchant_created_at), DAY) as vintage,
      area_name,
      zone_name,
      region_name,
      territory_name,
      merchant_created_at,
      std_category AS business_category,
      ST_GEOHASH(ST_GEOGPOINT(integ_longitude, integ_latitude),3) AS geohash3,
      ST_GEOHASH(ST_GEOGPOINT(integ_longitude, integ_latitude),4) AS geohash4,
      swipe_flag,
      speaker_flag,
      loan_flag,
      fp_flag,
      club_flag,
      qr_flag,
      credit_flag,
      debit_flag,
      actual_merchant_type AS merchant_acquisition_channel,
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
      merchant_created_at >= "2018-01-01"
      and MOD(merchant_id, {partitions})={i}
      AND merchant_id IN (
      SELECT
        merchant_id
      FROM (
        SELECT
      merchant_id,
      COUNT(DISTINCT id) AS txns,
      SUM(amount) AS tpv
    FROM
      bharatpe-analytics-prod.payin.transactions
    WHERE
      status = "SUCCESS"
      AND DATE(payment_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY
      1
    HAVING
      txns >=5 and tpv >= 150 ))
        """
        d[i] = pandas_gbq.read_gbq(merchant_data_mart_query)
    temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
    return temp

def trx1(partitions = 10):
    partitions = partitions
    d = {}
    for i in range(0, partitions):
        trx_query = f"""

    WITH
          merchant_list AS (
          SELECT
            pt.merchant_id,
            COUNT(DISTINCT id) AS txns,
            SUM(amount) AS tpv
          FROM
            bharatpe-analytics-prod.payin.transactions pt
          JOIN
            `bharatpe-analytics-prod.bharatpe_data_platfrom.universal_merchant_data_mart` dm
          ON
            pt.merchant_id = dm.merchant_id
          WHERE
            status = "SUCCESS"
            AND dm.merchant_created_at >= "2018-01-01"
            AND DATE(payment_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
          GROUP BY
            1
          HAVING
            txns >=5 and tpv >= 150),
          trx1 AS (
          SELECT
            merchant_id,
            DATE(payment_timestamp) AS payment_date,
            SUM(amount) AS daily_tpv,
            COUNT(DISTINCT payer_vpa_hash) AS daily_payees,
            COUNT(DISTINCT id) AS daily_txns,
            CURRENT_DATE() AS today
          FROM
            `bharatpe-analytics-prod.payin.transactions`
          WHERE
            status = "SUCCESS"
            AND merchant_id IN (
            SELECT
              merchant_id
            FROM
              merchant_list )
          GROUP BY
            1,
            2 )
        SELECT
          merchant_id,
          SUM(CASE
              WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 7 DAY) THEN daily_txns
            ELSE
            0
          END
            ) AS w1_txn_count,
          SUM(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 7 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 14 DAY) THEN daily_txns
            ELSE
            0
          END
            ) AS w2_txn_count,
          SUM(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 14 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 21 DAY) THEN daily_txns
            ELSE
            0
          END
            ) AS w3_txn_count,
          SUM(CASE
              WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_txns
            ELSE
            0
          END
            ) AS m1_txn_count,
          SUM(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_txns
            ELSE
            0
          END
            ) AS m2_txn_count,
          SUM(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_txns
            ELSE
            0
          END
            ) AS m3_txn_count,
          SUM(CASE
              WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_txns
            ELSE
            0
          END
            ) AS txn_count_90_days,
          SUM(CASE
              WHEN payment_date < today THEN daily_txns
            ELSE
            0
          END
            ) AS total_txn_count,
          COUNT(DISTINCT
            CASE
              WHEN payment_date < today THEN payment_date
            ELSE
            NULL
          END
            ) AS total_active_vintage,
          SUM(CASE
              WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 7 DAY) THEN daily_tpv
            ELSE
            0
          END
            ) AS w1_tpv,
          SUM(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 7 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 14 DAY) THEN daily_tpv
            ELSE
            0
          END
            ) AS w2_tpv,
          SUM(CASE
              WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_tpv
            ELSE
            0
          END
            ) AS m1_tpv,
          SUM(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_tpv
            ELSE
            0
          END
            ) AS m2_tpv,
          SUM(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_tpv
            ELSE
            0
          END
            ) AS m3_tpv,
          SUM(CASE
              WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_tpv
            ELSE
            0
          END
            ) AS tpv_90_days,
          SUM(CASE
              WHEN payment_date < today THEN daily_tpv
            ELSE
            0
          END
            ) AS total_tpv,
          COUNT(DISTINCT
            CASE
              WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN EXTRACT(DAY FROM DATE(payment_date))
            ELSE
            NULL
          END
            ) AS m1_active_days,
          COUNT(DISTINCT
            CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN EXTRACT(DAY FROM DATE(payment_date))
            ELSE
            NULL
          END
            ) AS m2_active_days,
          COUNT(DISTINCT
            CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN EXTRACT(DAY FROM DATE(payment_date))
            ELSE
            NULL
          END
            ) AS m3_active_days,
          MAX(CASE
              WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_tpv
            ELSE
            0
          END
            ) AS m1_max_daily_tpv,
          MAX(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_tpv
            ELSE
            0
          END
            ) AS m2_max_daily_tpv,
          MAX(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_tpv
            ELSE
            0
          END
            ) AS m3_max_daily_tpv,

          MAX(CASE
              WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_txns
            ELSE
            0
          END
            ) AS m1_max_daily_txns,
          MAX(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_txns
            ELSE
            0
          END
            ) AS m2_max_daily_txns,
          MAX(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_txns
            ELSE
            0
          END
            ) AS m3_max_daily_txns
        FROM
          trx1
        where MOD(merchant_id, {partitions})={i}

        GROUP BY
          1
        """
        d[i] = pandas_gbq.read_gbq(trx_query)
    temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
    return temp


def payee1(partitions = 10):
    partitions = partitions
    d = {}
    for i in range(0, partitions):
        trx_query = f"""
    WITH
      merchant_list AS (
      SELECT
        pt.merchant_id,
        COUNT(DISTINCT id) AS txns,
        SUM(amount) AS tpv
      FROM
        bharatpe-analytics-prod.payin.transactions pt
      JOIN
        `bharatpe-analytics-prod.bharatpe_data_platfrom.universal_merchant_data_mart` dm
      ON
        pt.merchant_id = dm.merchant_id
      WHERE
        status = "SUCCESS"
        AND dm.merchant_created_at >= "2018-01-01"
        AND DATE(payment_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      GROUP BY
        1
      HAVING
        txns >=5
        AND tpv >= 150),
      trx1 AS (
      SELECT
        merchant_id,
        DATE(payment_timestamp) AS payment_date,
        payer_vpa_hash AS daily_payees,
        CURRENT_DATE() AS today
      FROM
        `bharatpe-analytics-prod.payin.transactions`
      WHERE
        status = "SUCCESS"
        AND merchant_id IN (
        SELECT
          merchant_id
        FROM
          merchant_list ) )
    SELECT
      merchant_id,
      COUNT( DISTINCT
        CASE
          WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 7 DAY) THEN daily_payees
        ELSE
        NULL
      END
        ) AS w1_unique_payers,
      COUNT( DISTINCT
        CASE
          WHEN payment_date < DATE_SUB(today, INTERVAL 7 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 14 DAY) THEN daily_payees
        ELSE
        NULL
      END
        ) AS w2_unique_payers,
      COUNT( DISTINCT
        CASE
          WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_payees
        ELSE
        NULL
      END
        ) AS m1_unique_payers,
      COUNT( DISTINCT
        CASE
          WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_payees
        ELSE
        NULL
      END
        ) AS m2_unique_payers,
      COUNT( DISTINCT
        CASE
          WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_payees
        ELSE
        NULL
      END
        ) AS m3_unique_payers,
      COUNT( DISTINCT
        CASE
          WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_payees
        ELSE
        NULL
      END
        ) AS unique_payers_90_days,
      COUNT( DISTINCT
        CASE
          WHEN payment_date < today THEN daily_payees
        ELSE
        NULL
      END
        ) AS total_unique_payers,
    FROM
      trx1
    where MOD(merchant_id, {partitions})={i}
    GROUP BY
      1
        """
        d[i] = pandas_gbq.read_gbq(trx_query)
    temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
    return temp


def payee2(partitions = 10):
    partitions = partitions
    d = {}
    for i in range(0, partitions):
        trx_query = f"""
     WITH
          merchant_list AS (
          SELECT
            pt.merchant_id,
            COUNT(DISTINCT id) AS txns,
            SUM(amount) AS tpv
          FROM
            bharatpe-analytics-prod.payin.transactions pt
          JOIN
            `bharatpe-analytics-prod.bharatpe_data_platfrom.universal_merchant_data_mart` dm
          ON
            pt.merchant_id = dm.merchant_id
          WHERE
            status = "SUCCESS"
            AND dm.merchant_created_at >= "2018-01-01"
            AND DATE(payment_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
          GROUP BY
            1
          HAVING
            txns >=5 and tpv >= 150),
          trx1 AS (
          SELECT
            merchant_id,
            DATE(payment_timestamp) AS payment_date,
            COUNT(DISTINCT payer_vpa_hash) AS daily_payees,
            CURRENT_DATE() AS today
          FROM
            `bharatpe-analytics-prod.payin.transactions`
          WHERE
            status = "SUCCESS"
            AND merchant_id IN (
            SELECT
              merchant_id
            FROM
              merchant_list )
          GROUP BY
            1,
            2 )
        SELECT
          merchant_id,
          MAX(CASE
              WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_payees
            ELSE
            0
          END
            ) AS m1_max_daily_payees,
          MAX(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_payees
            ELSE
            0
          END
            ) AS m2_max_daily_payees,
          MAX(CASE
              WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_payees
            ELSE
            0
          END
            ) AS m3_max_daily_payees,

        FROM
          trx1
        where MOD(merchant_id, {partitions})={i}
        GROUP BY
          1
        """
        d[i] = pandas_gbq.read_gbq(trx_query)
    temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
    return temp


def app_sessions(partitions = 10):
    partitions = partitions
    d = {}
    for i in range(0, partitions):
        app_query = f"""
    WITH
      merchant_list AS (
      SELECT
        pt.merchant_id,
        dm.mid,
        COUNT(DISTINCT id) AS txns,
        SUM(amount) AS tpv
      FROM
        bharatpe-analytics-prod.payin.transactions pt
      JOIN
        `bharatpe-analytics-prod.bharatpe_data_platfrom.universal_merchant_data_mart` dm
      ON
        pt.merchant_id = dm.merchant_id
      WHERE
        status = "SUCCESS"
        AND dm.merchant_created_at >= "2018-01-01"
        AND DATE(payment_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      GROUP BY
        1,
        2
      HAVING
        txns >=5 and tpv >= 150),
      app1 AS (
      SELECT
        mid,
        DATE(created_at) AS created_at,
        CURRENT_DATE() AS today,
        COUNT(DISTINCT
          CASE
            WHEN event_name IN ('swipe_home_screen_landed', 'swipe_od_screen_landed', 'swipe_home_button_click_order_now', 'swipe_od_button_click_submit', 'bharat_swipe') THEN session_id
          ELSE
          NULL
        END
          ) AS swipe_sesions,
        COUNT(DISTINCT
          CASE
            WHEN event_name IN ('Loan_Enter_Pan_Page', 'Loan_Eligible_Page', 'Loan_Agreement_Page', 'Loan_Enach_Page', 'loans', 'loan', 'credit-score') THEN session_id
          ELSE
          NULL
        END
          ) AS loan_sesions,
        COUNT(DISTINCT
          CASE
            WHEN event_name IN ('bharatpe_speaker|bharatpe_speaker|speaker_home|faqs', 'bharatpe_speaker|bharatpe_speaker|new_merchant_home|onboarding_video|onboarding_video', 'speaker_place_order_clicked', 'bharatpe_speaker|bharatpe_speaker|address_details|address_selection|new_address', 'bharatpe_speaker|bharatpe_speaker|track_order|track_order', 'bharatpe_speaker|bharatpe_speaker|transactions|select_dates', 'speaker_payment_success', 'speaker_gst_landed', 'bharatpe_speaker|bharatpe_speaker|new_merchant_home|terms and conditions|terms and conditions', 'speaker_confirm_address_clicked', 'speaker_address_landed', 'bharatpe_club2|bharatpe_club2|club_home|speaker_free|know_more', 'speaker_payment_success', 'bharatpe_speaker|bharatpe_speaker|speaker_home|recent_trasactions|view_all', 'bharatpe_speaker|bharatpe_speaker|transaction_pending', 'speaker_address_landed', 'bharatpe_speaker|bharatpe_speaker|speaker_home|speaker_home', 'speaker_place_order_clicked', 'speaker_new_address_clicked', 'bharatpe_speaker', 'bharatpe_speaker|bharatpe_speaker|new_merchant_home|hero_introduction|order_now', 'bharatpe_speaker|bharatpe_speaker|new_merchant_home|hero_introduction', 'speaker_payment_landed', 'speaker_gst_landed', 'speaker_confirm_address_clicked', 'bharatpe_speaker|bharatpe_speaker|address_details|address_selection|OGL', 'bharatpe_speaker|bharatpe_speaker|new_merchant_home|join_club|join_club', 'speaker_order_now_landed', 'bharatpe_speaker|bharatpe_speaker|new_merchant_home|faqs', 'speaker_payment_landed', 'BharatPe Speaker', 'bharatpe_speaker|bharatpe_speaker|transactions', 'bharatpe_club2|bharatpe_club2|benefits_details|speaker_free_modal|ok_cta', 'bharatpe_speaker|bharatpe_speaker|new_merchant_home|intro_video|intro_video', 'bharatpe_club2|bharatpe_club2|benefits_details|speaker_free|know_more', 'bharatpe_speaker|bharatpe_speaker|transaction_success_flag', 'bharatpe_speaker|bharatpe_speaker|invoices', 'bharatpe_speaker|bharatpe_speaker|language', 'bharatpe_speaker|bharatpe_speaker|transaction_failed', 'speaker_join_club_clicked', 'bharatpe_speaker|bharatpe_speaker|speaker_home|service_request', 'bharatpe_speaker|bharatpe_speaker|share_cta|share_cta', 'speaker_order_now_landed', 'bharatpe_speaker|bharatpe_speaker|address_details|address_selection|address_card', 'bharatpe_speaker|bharatpe_speaker|language|radio_button', 'bharatpe_speaker|bharatpe_speaker|transaction_success', 'bharatpe_speaker|bharatpe_speaker|speaker_home|unboxing_video', 'bharatpe_speaker', 'BharatPe Speaker', 'bharatpe_speaker|bharatpe_speaker|speaker_home|invoices', 'speaker_new_address_clicked', 'bharatpe_speaker|bharatpe_speaker|track_order|track_status_cta', 'bharatpe_speaker|bharatpe_speaker|address_details|address_confirmation|confirm_address', 'bharatpe_speaker|bharatpe_speaker|invoices|select_invoice', 'bharatpe_speaker|bharatpe_speaker|speaker_home|laguage', 'speaker_join_club_clicked') THEN session_id
          ELSE
          NULL
        END
          ) AS speaker_sesions
      FROM
        bharatpe-analytics-prod.bharatpe_analytics_data.app_event
      WHERE
        DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)
      GROUP BY
        1,
        2 ),
      app2 AS (
      SELECT
        mid,
        SUM(CASE
            WHEN created_at < today AND created_at >= DATE_SUB(today, INTERVAL 7 DAY) THEN swipe_sesions
          ELSE
          0
        END
          ) AS w1_swipe_sessions,
        SUM(CASE
            WHEN created_at < today AND created_at >= DATE_SUB(today, INTERVAL 30 DAY) THEN swipe_sesions
          ELSE
          0
        END
          ) AS m1_swipe_sessions,
        SUM(CASE
            WHEN created_at < today AND created_at >= DATE_SUB(today, INTERVAL 7 DAY) THEN loan_sesions
          ELSE
          0
        END
          ) AS w1_loan_sessions,
        SUM(CASE
            WHEN created_at < today AND created_at >= DATE_SUB(today, INTERVAL 30 DAY) THEN loan_sesions
          ELSE
          0
        END
          ) AS m1_loan_sessions,
        SUM(CASE
            WHEN created_at < today AND created_at >= DATE_SUB(today, INTERVAL 7 DAY) THEN speaker_sesions
          ELSE
          0
        END
          ) AS w1_speaker_sessions,
        SUM(CASE
            WHEN created_at < today AND created_at >= DATE_SUB(today, INTERVAL 30 DAY) THEN speaker_sesions
          ELSE
          0
        END
          ) AS m1_speaker_sessions
      FROM
        app1
      GROUP BY
        1 )
    SELECT
      ml.merchant_id,
      w1_swipe_sessions,
      m1_swipe_sessions,
      w1_loan_sessions,
      m1_loan_sessions,
      w1_speaker_sessions,
      m1_speaker_sessions
    FROM
      app2
    JOIN
      merchant_list AS ml
    ON
      app2.mid = ml.mid
    WHERE MOD(merchant_id, {partitions})={i}
        """
        d[i] = pandas_gbq.read_gbq(app_query)
    temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
    return temp



# def trx_data(partitions = 10):
#     partitions = partitions
#     d = {}
#     for i in range(0, partitions):
#         trx_query = f"""
#      WITH
#       merchant_list AS (
#       SELECT
#         pt.merchant_id,
#         COUNT(DISTINCT id) AS txns,
#         SUM(amount) AS tpv
#       FROM
#         bharatpe-analytics-prod.payin.transactions pt
#       JOIN
#         `bharatpe-analytics-prod.bharatpe_data_platfrom.universal_merchant_data_mart` dm
#       ON
#         pt.merchant_id = dm.merchant_id
#       WHERE
#         status = "SUCCESS"
#         AND dm.merchant_created_at >= "2022-09-01"
#         AND DATE(payment_timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
#       GROUP BY
#         1
#       HAVING
#         txns >=5 and tpv >= 150),
#       trx1 AS (
#       SELECT
#         merchant_id,
#         DATE(payment_timestamp) AS payment_date,
#         SUM(amount) AS daily_tpv,
#         COUNT(DISTINCT payer_vpa_hash) AS daily_payees,
#         COUNT(DISTINCT id) AS daily_txns,
#         CURRENT_DATE() AS today
#       FROM
#         `bharatpe-analytics-prod.payin.transactions`
#       WHERE
#         status = "SUCCESS"
#         AND merchant_id IN (
#         SELECT
#           merchant_id
#         FROM
#           merchant_list )
#       GROUP BY
#         1,
#         2 )
#     SELECT
#       merchant_id,
#       SUM(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 7 DAY) THEN daily_payees
#         ELSE
#         0
#       END
#         ) AS w1_unique_payers,
#       SUM(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 7 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 14 DAY) THEN daily_payees
#         ELSE
#         0
#       END
#         ) AS w2_unique_payers,
#       SUM(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_payees
#         ELSE
#         0
#       END
#         ) AS m1_unique_payers,
#       SUM(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_payees
#         ELSE
#         0
#       END
#         ) AS m2_unique_payers,
#       SUM(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_payees
#         ELSE
#         0
#       END
#         ) AS m3_unique_payers,
#       SUM(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_payees
#         ELSE
#         0
#       END
#         ) AS unique_payers_90_days,
#       SUM(CASE
#           WHEN payment_date < today THEN daily_payees
#         ELSE
#         0
#       END
#         ) AS total_unique_payers,
#       SUM(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 7 DAY) THEN daily_txns
#         ELSE
#         0
#       END
#         ) AS w1_txn_count,
#       SUM(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 7 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 14 DAY) THEN daily_txns
#         ELSE
#         0
#       END
#         ) AS w2_txn_count,
#       SUM(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 14 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 21 DAY) THEN daily_txns
#         ELSE
#         0
#       END
#         ) AS w3_txn_count,
#       SUM(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_txns
#         ELSE
#         0
#       END
#         ) AS m1_txn_count,
#       SUM(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_txns
#         ELSE
#         0
#       END
#         ) AS m2_txn_count,
#       SUM(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_txns
#         ELSE
#         0
#       END
#         ) AS m3_txn_count,
#       SUM(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_txns
#         ELSE
#         0
#       END
#         ) AS txn_count_90_days,
#       SUM(CASE
#           WHEN payment_date < today THEN daily_txns
#         ELSE
#         0
#       END
#         ) AS total_txn_count,
#       COUNT(DISTINCT
#         CASE
#           WHEN payment_date < today THEN payment_date
#         ELSE
#         NULL
#       END
#         ) AS total_active_vintage,
#       SUM(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 7 DAY) THEN daily_tpv
#         ELSE
#         0
#       END
#         ) AS w1_tpv,
#       SUM(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 7 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 14 DAY) THEN daily_tpv
#         ELSE
#         0
#       END
#         ) AS w2_tpv,
#       SUM(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_tpv
#         ELSE
#         0
#       END
#         ) AS m1_tpv,
#       SUM(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_tpv
#         ELSE
#         0
#       END
#         ) AS m2_tpv,
#       SUM(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_tpv
#         ELSE
#         0
#       END
#         ) AS m3_tpv,
#       SUM(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_tpv
#         ELSE
#         0
#       END
#         ) AS tpv_90_days,
#       SUM(CASE
#           WHEN payment_date < today THEN daily_tpv
#         ELSE
#         0
#       END
#         ) AS total_tpv,
#       COUNT(DISTINCT
#         CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN EXTRACT(DAY FROM DATE(payment_date))
#         ELSE
#         NULL
#       END
#         ) AS m1_active_days,
#       COUNT(DISTINCT
#         CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN EXTRACT(DAY FROM DATE(payment_date))
#         ELSE
#         NULL
#       END
#         ) AS m2_active_days,
#       COUNT(DISTINCT
#         CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN EXTRACT(DAY FROM DATE(payment_date))
#         ELSE
#         NULL
#       END
#         ) AS m3_active_days,
#       MAX(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_tpv
#         ELSE
#         0
#       END
#         ) AS m1_max_daily_tpv,
#       MAX(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_tpv
#         ELSE
#         0
#       END
#         ) AS m2_max_daily_tpv,
#       MAX(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_tpv
#         ELSE
#         0
#       END
#         ) AS m3_max_daily_tpv,
#       MAX(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_payees
#         ELSE
#         0
#       END
#         ) AS m1_max_daily_payees,
#       MAX(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_payees
#         ELSE
#         0
#       END
#         ) AS m2_max_daily_payees,
#       MAX(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_payees
#         ELSE
#         0
#       END
#         ) AS m3_max_daily_payees,
#       MAX(CASE
#           WHEN payment_date < today AND payment_date >= DATE_SUB(today, INTERVAL 30 DAY) THEN daily_txns
#         ELSE
#         0
#       END
#         ) AS m1_max_daily_txns,
#       MAX(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 30 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 60 DAY) THEN daily_txns
#         ELSE
#         0
#       END
#         ) AS m2_max_daily_txns,
#       MAX(CASE
#           WHEN payment_date < DATE_SUB(today, INTERVAL 60 DAY) AND payment_date >= DATE_SUB(today, INTERVAL 90 DAY) THEN daily_txns
#         ELSE
#         0
#       END
#         ) AS m3_max_daily_txns
#     FROM
#       trx1
#     WHERE MOD(merchant_id, {partitions})={i}
#     GROUP BY
#       1
#         """
#         d[i] = pandas_gbq.read_gbq(trx_query)
#     temp = pd.concat([d[k] for k in d.keys()], axis = 0).reset_index(drop = True)
#     return temp