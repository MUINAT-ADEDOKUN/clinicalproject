-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC 
-- MAGIC clinicaltrial_2021_rdd1 = sc.textFile("/FileStore/tables/clinicaltrial_2021.csv")
-- MAGIC # extract header
-- MAGIC header = clinicaltrial_2021_rdd1.first()
-- MAGIC # filter out header row
-- MAGIC clinicaltrial_2021_rdd2 = clinicaltrial_2021_rdd1.filter(lambda row: row != header)
-- MAGIC colRDD = clinicaltrial_2021_rdd2.map(lambda line: line.split("|"))
-- MAGIC clinicaltrial_df = colRDD.toDF()
-- MAGIC clinicaltrial_df.show(4,truncate = False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_df = clinicaltrial_df.toDF("Id", "Sponsor", "Status", "Start", "Completion", "Type", "Submission", "Conditions", "Interventions")
-- MAGIC clinicaltrial_df.show(5,truncate =False )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC 
-- MAGIC pharma_df = spark.read.csv("/FileStore/tables/pharma.csv",header = True)

-- COMMAND ----------

-- MAGIC 
-- MAGIC %python                        
-- MAGIC clinicaltrial_df. createOrReplaceTempView ("sqlclinicaltrial_df")

-- COMMAND ----------

-- To check the content of the view
SELECT
  *
FROM
  sqlclinicaltrial_df
LIMIT
  10

-- COMMAND ----------

-- to check the databse 
SHOW DATABASES

-- COMMAND ----------


SHOW TABLES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  pharma_df. createOrReplaceTempView ("sqlpharma_df")

-- COMMAND ----------

-- to check the content of sqlpharma_df
SELECT
  *
FROM
  sqlpharma_df
LIMIT
  10

-- COMMAND ----------

--- create a permanent table clinicaltable
CREATE
OR REPLACE TABLE default.clinicaltable AS
SELECT
  *
FROM
  sqlclinicaltrial_df

-- COMMAND ----------

-- create a permanent table pharmatable
CREATE
OR REPLACE TABLE default.pharmatable AS
SELECT
  *
FROM
  sqlpharma_df

-- COMMAND ----------


SHOW TABLES

-- COMMAND ----------


CREATE DATABASE IF NOT EXISTS TRIALS_db

-- COMMAND ----------


SHOW DATABASES

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS trials_db.clinicaltable AS
SELECT
  *
FROM
  default.clinicaltable

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS trials_db.pharmatable AS
SELECT
  *
FROM
  default.pharmatable

-- COMMAND ----------


SHOW TABLES IN trials_db

-- COMMAND ----------

-- QUESTION 1 The number of studies in the dataset. You must --ensure that you explicitly check 
--distinct studies.
SELECT
  COUNT(DISTINCT Id) AS num_studies
FROM
  trials_db.clinicaltable
where
 Id != 'Id'

-- COMMAND ----------

-- Question 2 You should list all the types of studies in the 
--dataset along with the frequencies of each type. 
SELECT
  Type,
  COUNT(*) AS count
FROM
  trials_db.clinicaltable
WHERE
  Type != 'Type'
GROUP BY
  Type
ORDER BY
  count DESC

-- COMMAND ----------

-- Question 3 The top 5 conditions (from Conditions) with their frequencies.
SELECT
  Conditions,
  SUM(count) AS count
FROM
  (
    SELECT
      explode(split(Conditions, ',')) AS Conditions,
      COUNT(*) AS count
    FROM
      trials_db.clinicaltable
    WHERE
      Conditions != ''
    GROUP BY
      Conditions
  )
GROUP BY
  Conditions
ORDER BY
  count DESC
LIMIT
  5;

-- COMMAND ----------

-- Find the 10 most common sponsors that are not pharmaceutical companies, along 
--with the number of clinical trials they have sponsored.
SELECT
  Sponsor,
  COUNT(*) AS `number of sponsored Trial`
FROM
  trials_db.clinicaltable
WHERE
  Sponsor NOT IN (
    SELECT
      DISTINCT Parent_Company
    FROM
      trials_db.pharmatable
  )
GROUP BY
  Sponsor
ORDER BY
  `number of sponsored Trial` DESC
LIMIT
  10

-- COMMAND ----------


SELECT
  left(COMPLETION, 3) Month,
  count(*) completed_trials
FROM
  TRIALS_DB.CLINICALTABLE
WHERE
  right(completion, 4) = 2021
  AND status = 'Completed'
  AND completion != 'completion'
GROUP BY
  left(COMPLETION, 3)
ORDER BY
  CASE left(COMPLETION, 3)
    WHEN 'Jan' THEN 1
    WHEN 'Feb' THEN 2
    WHEN 'Mar' THEN 3
    WHEN 'Apr' THEN 4
    WHEN 'May' THEN 5
    WHEN 'Jun' THEN 6
    WHEN 'Jul' THEN 7
    WHEN 'Aug' THEN 8
    WHEN 'Sep' THEN 9
    WHEN 'Oct' THEN 10
    WHEN 'Nov' THEN 11
    WHEN 'Dec' THEN 12
  END ASC

-- COMMAND ----------

--top parent companies with the highest total penalty amount for each offense group
SELECT Parent_Company, CONCAT('$', FORMAT_NUMBER(k.Total_Penalty_Amount, 2)) AS Total_Penalty_Amount, Offense_Group
FROM (
    SELECT Parent_Company, Offense_Group, SUM(REGEXP_REPLACE(Penalty_Amount, '[$,]', '')) AS Total_Penalty_Amount
    FROM trials_db.pharmatable
    WHERE Penalty_Amount != '' 
    GROUP BY Parent_Company, Offense_Group
) k
ORDER BY k.Total_Penalty_Amount DESC

