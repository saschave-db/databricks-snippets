# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Find Delta tables that will benefit from VACUUM at scale
# MAGIC
# MAGIC Execute this script on a UC-enabled single-node cluster.

# COMMAND ----------

dbutils.widgets.text("retain_days", "7", "Retain history in days (min. 7 days)")
dbutils.widgets.text("target_table", "catalog.schema.table", "Target table")

# COMMAND ----------

tables = (
    spark.read.table("system.information_schema.tables")
    .filter("data_source_format = 'DELTA' AND table_catalog <> '__databricks_internal'")
    .select("table_catalog", "table_schema", "table_name", "table_owner", "last_altered")
    .collect()
)

display(tables)

# COMMAND ----------

import datetime

retain_days_wg = int(dbutils.widgets.get("retain_days"))
target_table_wg = dbutils.widgets.get("target_table")

if retain_days_wg < 7:
    retain_days_wg = 7

results = []

for idx, t in enumerate(tables):
    print(
        f"({idx+1}/{len(tables)}) Execute: VACUUM {t.table_catalog}.{t.table_schema}.{t.table_name} RETAIN {retain_days_wg*24} HOURS DRY RUN"
    )
    no_of_files = spark.sql(
        f"VACUUM {t.table_catalog}.{t.table_schema}.{t.table_name} RETAIN {retain_days_wg*24} HOURS DRY RUN"
    ).count()
    print(
        f"({idx+1}/{len(tables)}) {no_of_files} file(s) to vacuum"
    )
    if no_of_files > 0:
      results.append([t.table_catalog, t.table_schema, t.table_name, t.table_owner, t.last_altered, no_of_files, datetime.datetime.now()])

display(results)

# COMMAND ----------

cols = ['table_catalog', 'table_schema', 'table_name', 'table_owner','last_altered', 'no_files_to_vacuum', 'last_vacuum_check']
spark.createDataFrame(results).toDF(*cols).write.mode("append").saveAsTable(target_table_wg)

display(spark.read.table(target_table_wg))

# COMMAND ----------


