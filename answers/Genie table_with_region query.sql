-- Databricks notebook source
CREATE OR REPLACE FUNCTION `your-catalog`.`your-schema`.table_with_region()
RETURNS TABLE
RETURN
SELECT
  `region`,
  COUNT(*) AS churned_users_count
FROM
  (SELECT *, 
    CASE WHEN country IN ('USA', 'Canada', 'Mexico') THEN 'AMER'
    ELSE 'EMEA' 
END AS region
FROM   `your-catalog`.`your-schema`.`churn_users_silver`)
WHERE
  `churn` = TRUE
  AND `region` IS NOT NULL
GROUP BY
  `region`
ORDER BY
  churned_users_count DESC