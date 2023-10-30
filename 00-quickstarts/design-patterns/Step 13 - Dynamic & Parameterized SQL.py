# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview notebook on how to use newer functionality to make SQL more simple and dynamic in your Databricks pipelines!
# MAGIC
# MAGIC 1. Variables
# MAGIC 2. Parameters
# MAGIC 3. Structured Streaming
# MAGIC 4. Pushdown with Parameters

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## New SQL Variables
# MAGIC
# MAGIC - DBR 14.1+
# MAGIC - Not yet on DBSQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP DATABASE IF EXISTS main.iot_dashboard CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS main.iot_dashboard;
# MAGIC USE CATALOG main;
# MAGIC USE SCHEMA iot_dashboard;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLe main.iot_dashboard.bronze_users
# MAGIC (
# MAGIC userid BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC gender STRING,
# MAGIC age INT,
# MAGIC height DECIMAL(10,2), 
# MAGIC weight DECIMAL(10,2),
# MAGIC smoker STRING,
# MAGIC familyhistory STRING,
# MAGIC cholestlevs STRING,
# MAGIC bp STRING,
# MAGIC risk DECIMAL(10,2),
# MAGIC batch_id STRING,
# MAGIC batch_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC TBLPROPERTIES("delta.targetFileSize"="128mb") 
# MAGIC --LOCATION s3://<path>/
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Variables for batch processing
# MAGIC %sql
# MAGIC
# MAGIC DECLARE OR REPLACE VARIABLE batch_id STRING = uuid();
# MAGIC DECLARE OR REPLACE VARIABLE batch_process_timestamp TIMESTAMP = now();
# MAGIC
# MAGIC SELECT CONCAT('Loading batch ', batch_id, 'with timestamp : ', batch_process_timestamp) AS Status;
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Complex Data in Variables
# MAGIC %sql
# MAGIC
# MAGIC -- This is great for redability and reusability downstream
# MAGIC DECLARE OR REPLACE VARIABLE batch_metadata = named_struct('batch_id', uuid(), 'batch_timestamp', now());
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC batch_metadata.batch_id AS BatchId, 
# MAGIC batch_metadata.batch_timestamp AS BatchTimestamp;

# COMMAND ----------

# DBTITLE 1,Load Some Data Tagging the Load Batch
# MAGIC %sql
# MAGIC
# MAGIC -- Notice we dont HAVE to define the DDL upfront anymore as of DBR 11.2+
# MAGIC CREATE TABLE IF NOT EXISTS main.iot_dashboard.bronze_sensors;
# MAGIC
# MAGIC COPY INTO main.iot_dashboard.bronze_sensors
# MAGIC FROM (SELECT 
# MAGIC       id::bigint AS Id,
# MAGIC       device_id::integer AS device_id,
# MAGIC       user_id::integer AS user_id,
# MAGIC       calories_burnt::decimal(10,2) AS calories_burnt, 
# MAGIC       miles_walked::decimal(10,2) AS miles_walked, 
# MAGIC       num_steps::decimal(10,2) AS num_steps, 
# MAGIC       timestamp::timestamp AS timestamp,
# MAGIC       value  AS value, -- This is a JSON object,
# MAGIC       batch_metadata.batch_id AS batch_id,
# MAGIC       batch_metadata.batch_timestamp AS batch_timestamp
# MAGIC FROM "/databricks-datasets/iot-stream/data-device/")
# MAGIC FILEFORMAT = json -- csv, xml, txt, parquet, binary, etc.
# MAGIC COPY_OPTIONS('force'='true', 'mergeSchema'='true') ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Look at batch data
# MAGIC %sql
# MAGIC
# MAGIC -- Single batch gets a single id
# MAGIC -- timestamp is by batch
# MAGIC SELECT * FROM main.iot_dashboard.bronze_sensors;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Define Silver Table
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS main.iot_dashboard.silver_sensors
# MAGIC AS 
# MAGIC SELECT * FROM main.iot_dashboard.bronze_sensors
# MAGIC WHERE 1=0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Get max process timestamp in silver table
# MAGIC DECLARE OR REPLACE VARIABLE watermark_timestamp TIMESTAMP DEFAULT '1900-01-01 00:00:00'::timestamp;
# MAGIC
# MAGIC -- If using a dynamic query, declare it first then set it
# MAGIC SET VARIABLE watermark_timestamp = 
# MAGIC  (SELECT COALESCE(MAX(batch_timestamp)::timestamp, '1900-01-01 00:00:00'::timestamp) FROM main.iot_dashboard.silver_sensors);
# MAGIC

# COMMAND ----------

# DBTITLE 1,Look at Silver Status
# MAGIC %sql
# MAGIC
# MAGIC SELECT watermark_timestamp;

# COMMAND ----------

# DBTITLE 1,Load Batch
# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO main.iot_dashboard.silver_sensors AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC   *
# MAGIC   FROM main.iot_dashboard.bronze_sensors
# MAGIC   WHERE batch_timestamp > watermark_timestamp -- use variables for batching and session level info!
# MAGIC ) AS source
# MAGIC ON source.id = target.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC ANALYZE TABLE main.iot_dashboard.silver_sensors COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC
# MAGIC OPTIMIZE main.iot_dashboard.silver_sensors ZORDER BY (id, `timestamp`);
# MAGIC

# COMMAND ----------

# DBTITLE 1,Update Variable
# MAGIC %sql
# MAGIC
# MAGIC SET VARIABLE watermark_timestamp = 
# MAGIC  (SELECT COALESCE(MAX(batch_timestamp)::timestamp, '1900-01-01 00:00:00'::timestamp) FROM main.iot_dashboard.silver_sensors);
# MAGIC
# MAGIC SELECT watermark_timestamp;

# COMMAND ----------

# DBTITLE 1,Get Batch Ids in process - ? Buggy no complex types list a list?
# MAGIC %sql
# MAGIC
# MAGIC DECLARE OR REPLACE VARIABLE active_batches ARRAY<STRING> DEFAULT ARRAY('');
# MAGIC
# MAGIC SET VAR active_batches = (SELECT collect_list(batch_id) FROM main.iot_dashboard.bronze_sensors);
# MAGIC
# MAGIC SELECT active_batches;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG main;
# MAGIC USE SCHEMA iot_dashboard;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE param_configs
# MAGIC (id INT, config MAP< STRING, STRING>)
# MAGIC ;
# MAGIC
# MAGIC INSERT INTO param_configs
# MAGIC VALUES (1, map('timestamp', '1900-01-01 00:00:00', 'user_id', '1'));
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC map_keys(config)
# MAGIC FROM param_configs
# MAGIC WHERE id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION 
