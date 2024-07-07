-- Out come: Cohort Customer Retention 
-- Shows the number of new customers each month and the return purchase rate of these customer groups for each subsequent month
-- Data input: Transaction data (2023 - NOW)
--------------
--------------
-- Show sale data table & find the previous transaction date of each customer
WITH transaction AS
(
    SELECT transaction.customer_id,
            transaction.date_created, 
            transaction.id, 
            transaction.revenue,
            lag(transaction.date_created, 1) OVER(
                    PARTITION BY transaction.customer_id
                    ORDER BY
                        transaction.date_created
                ) AS previous_date
    FROM transaction
    WHERE transaction.date_created BETWEEN DATE_ADD('DAY', -400, NOW()) AND NOW()
        AND LOWER(transaction.transaction_sale_type) LIKE '%sale%'
)
-- Find the month of each customer's first purchase
,temp_transaction_retention AS 
(
    SELECT
        temp_transaction.customer_id, 
        temp_transaction.transaction_month, 
        CASE
            WHEN temp_transaction.previous_date IS NULL
            OR date_diff('day', temp_transaction.previous_date, temp_transaction.date_created) > 365 THEN 1
            ELSE 0
        END AS temp_retention 
    FROM
        temp_transaction
) 
, cohort_population AS 
(    
    SELECT
            temp_transaction_retention.customer_id,
            temp_transaction_retention.transaction_month AS cohort_month
    FROM
        temp_transaction_retention
    WHERE temp_transaction_retention.temp_retention = 1 
        GROUP BY temp_transaction_retention.customer_id,
            temp_transaction_retention.transaction_month
)
, population AS 
(
    SELECT pre.cohort_month, 
            COUNT(DISTINCT pre.customer_id) AS cohort_population
    FROM pre 
    GROUP BY pre.cohort_month
)
, mart_ AS (
    SELECT
        transaction.customer_id,
        population.cohort_month,
        transaction.id AS transaction_id,
        transaction.date_created,
        transaction.revenue,
        DATE_DIFF(
            'MONTH',
            pre.cohort_month,
            DATE_TRUNC('MONTH', transaction.date_created)
        ) AS month_number,
        population.cohort_population
    FROM
        transaction
        JOIN pre ON transaction.customer_id = pre.customer_id
        JOIN population ON population.cohort_month = pre.cohort_month
    WHERE transaction.date_created >= population.cohort_month
        AND DATE_TRUNC('month', population.cohort_month) >= DATE_ADD('month', -13, DATE_TRUNC('month', NOW()))
         -- ORDER BY pre.total_transaction DESC, transaction.customer_id, transaction.date_created
)
-- Calculate return rate by month
, final_ AS (
    SELECT
        mart_.cohort_month,
        mart_.month_number,
        CAST(MAX(mart_.cohort_population) AS DOUBLE) AS cohort_population,
        CAST(COUNT(DISTINCT mart_.customer_id) AS DOUBLE) AS cohort_retention_number,
        SUM(mart_.revenue) revenue,
        COUNT(DISTINCT mart_.transaction_id) AS transactions
    FROM
        mart_
    GROUP BY
        mart_.cohort_month,
        mart_.month_number -- ORDER BY mart_.cohort_month, mart_.month_numbe
)
SELECT
    final_.cohort_month,
    final_.month_number,
    final_.cohort_retention_number,
    final_.cohort_population,
    CAST(
        (
            final_.cohort_retention_number / final_.cohort_population
        ) AS DECIMAL(7, 3)
    ) AS cohort_retention_rate,
    final_.revenue,
    final_.transactions
FROM
    final_
ORDER BY
    cohort_month,
    month_number
