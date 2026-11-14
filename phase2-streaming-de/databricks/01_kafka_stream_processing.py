# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # AtliQ Phase 2 (LEARNER STARTER) — Kafka → Delta with Structured Streaming
# MAGIC Complete the TODOs to build Bronze → Silver → Gold as **streams**.
# MAGIC
# MAGIC **Free Edition (serverless) rules:** checkpoints go in a Unity Catalog
# MAGIC **Volume** (no DBFS), streams write to **managed tables**, and every stream
# MAGIC needs its **own** checkpoint folder.
# MAGIC
# MAGIC **Triggers on serverless:** always-on (processing-time) triggers are not
# MAGIC available, so every stream uses `.trigger(availableNow=True)`: it processes
# MAGIC everything new since its checkpoint, then stops. Follow each `.toTable(...)`
# MAGIC with `query.awaitTermination()`. While the producer runs, re-run the notebook
# MAGIC (or schedule it as a job every few minutes); the checkpoints make each run
# MAGIC pick up exactly where the last one stopped.
# MAGIC
# MAGIC **Running on Azure Databricks instead (guide section 3.1)?** Use
# MAGIC `.trigger(processingTime="30 seconds")` and do **not** call
# MAGIC `awaitTermination()`: the streams keep running while the producer sends events.
# MAGIC When you finish, stop them with `for q in spark.streams.active: q.stop()` and
# MAGIC terminate the cluster.

# COMMAND ----------

# DBTITLE 1,Create Catalog, schema and  volume
KAFKA_BOOTSTRAP="pkc-56d1g.eastus.azure.confluent.cloud:9092"
KAFKA_API_KEY="QNXJ56EZ4APWCUY6"
KAFKA_API_SECRET="cfltyJUHm1ubRjUtN+wYB3Cw0sLgVrZ+3dubZzb3Xz7AcnY10fRs6MfF30KtYqFQ"
KAFKA_TOPIC="atliq.orders.events"


CATALOG, SCHEMA = "atliq", "streaming"
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# One-time setup (given):
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

# MAGIC %md ## TASK 1 — Bronze: raw events off Kafka, no parsing
# MAGIC Read the topic with `spark.readStream.format("kafka")` and append the raw
# MAGIC records to `atliq.streaming.bronze_order_events`.
# MAGIC
# MAGIC Hints:
# MAGIC - Options you need: `kafka.bootstrap.servers`, `subscribe`,
# MAGIC   `startingOffsets = earliest`, `kafka.security.protocol = SASL_SSL`,
# MAGIC   `kafka.sasl.mechanism = PLAIN`, and `kafka.sasl.jaas.config`
# MAGIC   (on Databricks the login module class is
# MAGIC   `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule`).
# MAGIC - Kafka gives you binary key/value — CAST both to STRING.
# MAGIC - Keep topic, partition, offset, timestamp columns too. Bronze keeps everything.
# MAGIC - writeStream: outputMode "append", `.trigger(availableNow=True)` (Azure: see above),
# MAGIC   checkpointLocation f"{CKPT}/bronze", .toTable(...), then
# MAGIC   `query.awaitTermination()`

# COMMAND ----------

# DBTITLE 1,Building Bronze stream
# TODO: Task 1 — your Bronze stream here


from pyspark.sql import functions as F

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
    f'required username="{KAFKA_API_KEY}" '
    f'password="{KAFKA_API_SECRET}";'
)

bronze_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .load()
    .select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        "topic",
        "partition",
        "offset",
        "timestamp"
    )
)

bronze_query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/bronze")
    .trigger(availableNow=True)
    .toTable("atliq.streaming.bronze_order_events")
)

bronze_query.awaitTermination()



# COMMAND ----------

# DBTITLE 1,Checking Bronze Stream
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_events
# MAGIC FROM atliq.streaming.bronze_order_events;

# COMMAND ----------

# DBTITLE 1,checking bronze events
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM atliq.streaming.bronze_order_events LIMIT 10;

# COMMAND ----------

# MAGIC %md ## TASK 2 — Silver: parse, de-duplicate, handle late data
# MAGIC Stream FROM the Bronze table into `atliq.streaming.silver_order_events`:
# MAGIC 1. Parse the JSON value with an explicit schema (event_id, event_type,
# MAGIC    event_ts, order_id, customer_id, city, product_id, quantity,
# MAGIC    order_amount, payment_method).
# MAGIC 2. Convert event_ts to a real timestamp.
# MAGIC 3. The producer sends a few broken records on purpose: from_json returns NULL for
# MAGIC    fields it cannot read. Drop (or write to a quarantine table) rows where event_id,
# MAGIC    event_ts or order_amount is NULL, **before** the watermark.
# MAGIC 4. Add a **10-minute watermark** on event_ts, then
# MAGIC    **dropDuplicates(["event_id", "event_ts"])** — so a replayed event can never land twice.
# MAGIC    Including the event-time column lets Spark forget old event_ids once the
# MAGIC    watermark passes them, so the de-dup state stays small.
# MAGIC    (`dropDuplicatesWithinWatermark(["event_id"])` is also accepted.)
# MAGIC
# MAGIC Hint: `spark.readStream.table(...)`, `F.from_json`, `withWatermark`,
# MAGIC `.trigger(availableNow=True)`, checkpoint f"{CKPT}/silver".

# COMMAND ----------

# DBTITLE 1,Sliver stream
# TODO: Task 2 — your Silver stream here

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType, DoubleType
)

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("order_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("city", StringType()),
    StructField("product_id", LongType()),
    StructField("quantity", IntegerType()),
    StructField("order_amount", DoubleType()),
    StructField("payment_method", StringType())
])

silver_df = (
    spark.readStream
    .table("atliq.streaming.bronze_order_events")
    .withColumn("parsed", F.from_json(F.col("value"), event_schema))
    .select("parsed.*")
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .filter(
        F.col("event_id").isNotNull()
        & F.col("event_ts").isNotNull()
        & F.col("order_amount").isNotNull()
    )
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["event_id", "event_ts"])
)

CATALOG = "atliq"
SCHEMA = "streaming"
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

silver_query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/silver")
    .trigger(availableNow=True)
    .toTable("atliq.streaming.silver_order_events")
)

silver_query.awaitTermination()


# COMMAND ----------

# DBTITLE 1,Checking Silver stream
# MAGIC %sql
# MAGIC SELECT event_type, COUNT(*) AS total_events
# MAGIC FROM atliq.streaming.silver_order_events
# MAGIC GROUP BY event_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_events
# MAGIC FROM atliq.streaming.silver_order_events;

# COMMAND ----------

# MAGIC %md ## TASK 3 — Gold: the live revenue ticker
# MAGIC From the Silver stream, keep only `payment_received` events and aggregate
# MAGIC into **5-minute tumbling windows**: orders_paid = count, revenue = sum of
# MAGIC order_amount. Append closed windows to `atliq.streaming.gold_revenue_5min`.
# MAGIC
# MAGIC Hints:
# MAGIC - A stream read from a table does **not** inherit Silver's watermark.
# MAGIC   Declare `withWatermark("event_ts", "10 minutes")` again before the window,
# MAGIC   or the append-mode aggregation will refuse to start.
# MAGIC - `F.window("event_ts", "5 minutes")` returns a struct. Flatten it:
# MAGIC   `F.col("window.start").alias("window_start")`,
# MAGIC   `F.col("window.end").alias("window_end")` (the Verify query uses `window_start`).
# MAGIC - `.trigger(availableNow=True)`, checkpoint f"{CKPT}/gold".
# MAGIC - Think about WHY a window only appears after the watermark passes its end
# MAGIC   (you will explain this in your write-up). With availableNow, the newest
# MAGIC   windows usually appear on a later run, once newer events move the watermark.

# COMMAND ----------

# TODO: Task 3 — your Gold stream here


from pyspark.sql import functions as F

# Checkpoint configuration
CATALOG = "atliq"
SCHEMA = "streaming"
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# Read Silver as a stream and keep successful payments
gold_df = (
    spark.readStream
    .table("atliq.streaming.silver_order_events")
    .filter(F.col("event_type") == "payment_received")
    .withWatermark("event_ts", "10 minutes")
    .groupBy(F.window("event_ts", "5 minutes"))
    .agg(
        F.count("*").alias("orders_paid"),
        F.sum("order_amount").alias("revenue")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "orders_paid",
        "revenue"
    )
)

# Write completed windows to Gold
gold_query = (
    gold_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/gold")
    .trigger(availableNow=True)
    .toTable("atliq.streaming.gold_revenue_5min")
)

gold_query.awaitTermination()



# COMMAND ----------

# DBTITLE 1,Gold stream
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM atliq.streaming.gold_revenue_5min
# MAGIC ORDER BY window_start;

# COMMAND ----------

# MAGIC %md ## Verify (given)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_type, COUNT(*) AS events
# MAGIC FROM atliq.streaming.silver_order_events GROUP BY event_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM atliq.streaming.gold_revenue_5min ORDER BY window_start DESC LIMIT 12;