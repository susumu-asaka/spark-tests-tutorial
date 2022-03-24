import delta
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
import datetime as dt

db_name = "my_db"
bronze_table_name = f"{db_name}.health_tracker_bronze"
silver_table_name = f"{db_name}.health_tracker_silver"


def write_silver_table(spark: SparkSession):
    """Write rows from Bronze table into Silver table

    * Load rows from Bronze table that was not loaded yet (status = "new")
    * Extract and transform the raw string to columns
    * Write result into Silver table
    * Update the `status` of the loaded rows from Bronze table to "loaded"
    """
    bronze_df = spark.table(bronze_table_name).filter("status = 'new'")
    if bronze_df.first():
        # bronze_df is not empty
        json_schema = "time TIMESTAMP, name STRING, steps INTEGER"
        bronze_augmented_df = (bronze_df
                               .withColumn("nested_json",
                                           f.from_json("value", json_schema)))
        silver_df = (bronze_augmented_df
                     .select(f.col("nested_json.time").alias("event_time"),
                             f.col("nested_json.name").alias("name"),
                             f.col("nested_json.steps").alias("steps"),
                             f.lit(str(dt.date.today())).cast("DATE")
                             .alias("p_ingest_date")))
        (silver_df.write.format("delta").mode("append")
         .partitionBy("p_ingest_date").saveAsTable(silver_table_name))

        bronze_table = delta.DeltaTable.forName(spark, bronze_table_name)
        update_bronze_table_status(bronze_table, bronze_df, "loaded")


def update_bronze_table_status(table: delta.DeltaTable, source: DataFrame,
                               status: str) -> None:
    """Update column `status` of `table`.

    Merge into `table` using `source` by  "value"
    when matched update set status = `status`.
    """
    (table.alias("tgt").merge(source.alias("src"), "tgt.value = src.value")
     .whenMatchedUpdate(set={"status": f"'{status}'"}).execute())
