# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Leverage information_schema and Delta history to find candidates for OPTIMIZE and VACUUM

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

# set thresholds
threshold_days = 3
threshold_versions = 10

# select catalog
catalog = "<name-of-your-catalog>"

agg_expr = [
    max(x).alias(x) for x in ["version", "opt_version", "timestamp", "opt_timestamp"]
]

tables = (
    spark.read.table("{}.information_schema.tables".format(catalog))
    .filter((col("table_type") != "VIEW") & (col("data_source_format") == "DELTA"))
    .select("table_catalog", "table_schema", "table_name")
).collect()

for t in tables:
    deltaTable = DeltaTable.forName(spark, ".".join(t))
    history = deltaTable.history()

    is_candidate = (
        history.select("version", "timestamp", 
                       when(col("operation") == "OPTIMIZE", col("version")).alias("opt_version"),
                       when(col("operation") == "OPTIMIZE", col("timestamp")).alias("opt_timestamp"))
        .groupBy()
        .agg(*agg_expr)
        .select(
            (col("version") - col("opt_version")).alias("version_diff"),
            datediff(col("timestamp"), col("opt_timestamp")).alias("day_diff"),
        )
        .filter(
            (col("version_diff") >= threshold_versions) & (col("day_diff") >= threshold_days)
        )
        .count()
    )

    if is_candidate == 1:
      print("Optimize candidate: {}".format(".".join(t)))
      # next step could be informing the table owner and recommend running OPTIMIZE and VACUUM

# COMMAND ----------


