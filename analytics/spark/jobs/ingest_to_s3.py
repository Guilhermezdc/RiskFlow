"""
analytics/spark/jobs/ingest_to_s3.py

Job Spark: consome decisions do Kafka → converte para Parquet → salva no S3.

Estrutura S3:
  s3://{bucket}/raw/transactions/year=YYYY/month=MM/day=DD/hour=HH/
  s3://{bucket}/raw/decisions/year=YYYY/month=MM/day=DD/hour=HH/

Execução:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    ingest_to_s3.py
"""

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, ArrayType
)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────

DECISION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("risk_score", DoubleType(), True),
    StructField("triggered_rules", ArrayType(StringType()), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("decision_timestamp", StringType(), True),
    StructField("processing_latency_ms", LongType(), True),
])

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("merchant_country", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("is_international", BooleanType(), True),
    StructField("is_new_device", BooleanType(), True),
    StructField("channel", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_time_ms", LongType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(os.getenv("SPARK_APP_NAME", "risk-flow-ingest-s3"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def ingest_decisions_to_s3(spark: SparkSession):
    """
    Lê decisions do Kafka (streaming) e escreve no S3 como Parquet particionado.
    Roda como Structured Streaming com trigger de micro-batch.
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    decisions_topic = os.getenv("KAFKA_TOPIC_DECISIONS", "transactions.decisions")
    bucket = os.getenv("S3_BUCKET", "risk-flow-datalake")
    s3_prefix = os.getenv("S3_PREFIX_DECISIONS", "raw/decisions")
    checkpoint_dir = f"s3a://{bucket}/checkpoints/decisions"

    # ── Leitura do Kafka ──
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", decisions_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON ──
    decisions_df = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), DECISION_SCHEMA).alias("d"))
        .select("d.*")
        # Adiciona colunas de partição a partir do timestamp
        .withColumn("decision_ts", F.to_timestamp("decision_timestamp"))
        .withColumn("year", F.year("decision_ts"))
        .withColumn("month", F.format_string("%02d", F.month("decision_ts")))
        .withColumn("day", F.format_string("%02d", F.dayofmonth("decision_ts")))
        .withColumn("hour", F.format_string("%02d", F.hour("decision_ts")))
        .drop("decision_ts")
    )

    # ── Escrita no S3 ──
    query = (
        decisions_df.writeStream
        .format("parquet")
        .option("path", f"s3a://{bucket}/{s3_prefix}")
        .option("checkpointLocation", checkpoint_dir)
        .partitionBy("year", "month", "day", "hour")
        .trigger(processingTime="5 minutes")   # micro-batch a cada 5 minutos
        .outputMode("append")
        .start()
    )

    return query


def ingest_transactions_to_s3(spark: SparkSession):
    """
    Lê transações brutas do Kafka e escreve no S3 (para auditoria e replay).
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    transactions_topic = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions.raw")
    bucket = os.getenv("S3_BUCKET", "risk-flow-datalake")
    s3_prefix = os.getenv("S3_PREFIX_RAW", "raw/transactions")
    checkpoint_dir = f"s3a://{bucket}/checkpoints/transactions"

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", transactions_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    txs_df = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("t"))
        .select("t.*")
        .withColumn("event_ts", F.to_timestamp(F.col("event_time_ms") / 1000))
        .withColumn("year", F.year("event_ts"))
        .withColumn("month", F.format_string("%02d", F.month("event_ts")))
        .withColumn("day", F.format_string("%02d", F.dayofmonth("event_ts")))
        .withColumn("hour", F.format_string("%02d", F.hour("event_ts")))
        .drop("event_ts")
    )

    query = (
        txs_df.writeStream
        .format("parquet")
        .option("path", f"s3a://{bucket}/{s3_prefix}")
        .option("checkpointLocation", checkpoint_dir)
        .partitionBy("year", "month", "day", "hour")
        .trigger(processingTime="5 minutes")
        .outputMode("append")
        .start()
    )

    return query


def main():
    spark = create_spark_session()
    logger.info("Iniciando ingestão Kafka → S3 (Parquet)")

    q1 = ingest_transactions_to_s3(spark)
    q2 = ingest_decisions_to_s3(spark)

    logger.info("Streams ativos. Aguardando...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
