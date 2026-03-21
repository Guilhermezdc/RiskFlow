"""
analytics/spark/jobs/s3_to_redshift.py

Job Spark Batch: S3 (Parquet) → AWS Redshift (raw → staging → mart)

Execução diária (via Airflow ou cron):
  spark-submit s3_to_redshift.py --date 2024-01-15
"""

import argparse
import logging
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("risk-flow-s3-to-redshift")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.jars.packages", "io.github.spark-redshift-community:spark-redshift_2.12:6.2.0-spark_3.5")
        .getOrCreate()
    )


def redshift_options(table: str, schema: str = "raw") -> dict:
    """Retorna as opções de conexão com Redshift."""
    return {
        "url": f"jdbc:redshift://{os.getenv('REDSHIFT_HOST')}:{os.getenv('REDSHIFT_PORT', '5439')}/{os.getenv('REDSHIFT_DB')}",
        "dbtable": f"{schema}.{table}",
        "user": os.getenv("REDSHIFT_USER"),
        "password": os.getenv("REDSHIFT_PASSWORD"),
        "tempdir": f"s3a://{os.getenv('S3_BUCKET')}/redshift-temp/",
        "aws_iam_role": os.getenv("REDSHIFT_IAM_ROLE"),
    }


# ──────────────────────────────────────────────
# LAYER 1: RAW — carga incremental direta do S3
# ──────────────────────────────────────────────

def load_raw(spark: SparkSession, date_str: str):
    """
    Carrega dados brutos do S3 para a camada raw do Redshift.
    Sem transformações — dados exatamente como vieram do Kafka.
    """
    year, month, day = date_str.split("-")
    bucket = os.getenv("S3_BUCKET", "risk-flow-datalake")

    # Transações
    txs_path = f"s3a://{bucket}/raw/transactions/year={year}/month={month}/day={day}/"
    txs_df = spark.read.parquet(txs_path)

    (
        txs_df.write
        .format("io.github.spark_redshift_community.spark.redshift")
        .options(**redshift_options("transactions", "raw"))
        .mode("append")
        .save()
    )
    logger.info(f"RAW: {txs_df.count():,} transações carregadas")

    # Decisions
    dec_path = f"s3a://{bucket}/raw/decisions/year={year}/month={month}/day={day}/"
    dec_df = spark.read.parquet(dec_path)

    (
        dec_df.write
        .format("io.github.spark_redshift_community.spark.redshift")
        .options(**redshift_options("decisions", "raw"))
        .mode("append")
        .save()
    )
    logger.info(f"RAW: {dec_df.count():,} decisions carregadas")

    return txs_df, dec_df


# ──────────────────────────────────────────────
# LAYER 2: STAGING — limpeza e padronização
# ──────────────────────────────────────────────

def transform_staging(spark: SparkSession, txs_df: DataFrame, dec_df: DataFrame):
    """
    Aplica limpeza, tipagem e join transações + decisions.
    Resultado: staging.fact_transactions (tabela limpa e enriquecida).
    """

    # Normaliza transações
    clean_txs = (
        txs_df
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("amount") > 0)
        .withColumn("amount", F.round(F.col("amount"), 2))
        .withColumn("currency", F.upper(F.col("currency")))
        .withColumn("country_code", F.upper(F.col("country_code")))
        .withColumn("merchant_country", F.upper(F.col("merchant_country")))
        .withColumn("event_date", F.to_date(F.to_timestamp(F.col("event_time_ms") / 1000)))
        .withColumn("event_hour", F.hour(F.to_timestamp(F.col("event_time_ms") / 1000)))
        .dropDuplicates(["transaction_id"])
    )

    # Normaliza decisions
    clean_dec = (
        dec_df
        .filter(F.col("transaction_id").isNotNull())
        .select(
            "transaction_id",
            F.col("status").alias("decision_status"),
            F.col("risk_score"),
            F.col("triggered_rules"),
            F.col("processing_latency_ms"),
            F.col("decision_timestamp"),
        )
        .dropDuplicates(["transaction_id"])
    )

    # Join transações + decisions
    fact_df = (
        clean_txs.join(clean_dec, on="transaction_id", how="left")
        .withColumn(
            "decision_status",
            F.coalesce(F.col("decision_status"), F.lit("PENDING"))
        )
    )

    (
        fact_df.write
        .format("io.github.spark_redshift_community.spark.redshift")
        .options(**redshift_options("fact_transactions", "staging"))
        .mode("append")
        .save()
    )
    logger.info(f"STAGING: {fact_df.count():,} registros em fact_transactions")

    return fact_df


# ──────────────────────────────────────────────
# LAYER 3: MART — agregados para BI / reporting
# ──────────────────────────────────────────────

def build_mart(spark: SparkSession, fact_df: DataFrame):
    """
    Constrói as tabelas analíticas (mart) para consumo por BI.
    """

    # ── mart.daily_risk_summary ──
    daily_summary = (
        fact_df
        .groupBy("event_date", "decision_status")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.avg("risk_score").alias("avg_risk_score"),
            F.avg("processing_latency_ms").alias("avg_latency_ms"),
            F.percentile_approx("processing_latency_ms", 0.95).alias("p95_latency_ms"),
        )
        .orderBy("event_date", "decision_status")
    )

    (
        daily_summary.write
        .format("io.github.spark_redshift_community.spark.redshift")
        .options(**redshift_options("daily_risk_summary", "mart"))
        .mode("append")
        .save()
    )
    logger.info(f"MART: daily_risk_summary — {daily_summary.count()} linhas")

    # ── mart.customer_risk_profile ──
    customer_profile = (
        fact_df
        .groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.avg("risk_score").alias("avg_risk_score"),
            F.max("risk_score").alias("max_risk_score"),
            F.sum(F.when(F.col("decision_status") == "REJECT", 1).otherwise(0)).alias("total_rejections"),
            F.sum(F.when(F.col("decision_status") == "MANUAL_REVIEW", 1).otherwise(0)).alias("total_reviews"),
            F.max("event_date").alias("last_transaction_date"),
        )
        .withColumn("rejection_rate", F.col("total_rejections") / F.col("total_transactions"))
    )

    (
        customer_profile.write
        .format("io.github.spark_redshift_community.spark.redshift")
        .options(**redshift_options("customer_risk_profile", "mart"))
        .mode("overwrite")
        .save()
    )
    logger.info(f"MART: customer_risk_profile — {customer_profile.count()} clientes")

    # ── mart.rule_effectiveness ──
    rule_effectiveness = (
        fact_df
        .filter(F.col("triggered_rules").isNotNull())
        .withColumn("rule", F.explode("triggered_rules"))
        .groupBy("rule", "event_date")
        .agg(
            F.count("transaction_id").alias("trigger_count"),
            F.avg("risk_score").alias("avg_risk_score_when_triggered"),
            F.sum(F.when(F.col("decision_status") == "REJECT", 1).otherwise(0)).alias("led_to_reject"),
            F.sum(F.when(F.col("decision_status") == "MANUAL_REVIEW", 1).otherwise(0)).alias("led_to_review"),
        )
    )

    (
        rule_effectiveness.write
        .format("io.github.spark_redshift_community.spark.redshift")
        .options(**redshift_options("rule_effectiveness", "mart"))
        .mode("append")
        .save()
    )
    logger.info(f"MART: rule_effectiveness — {rule_effectiveness.count()} linhas")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main(date_str: str):
    spark = create_spark_session()
    logger.info(f"Iniciando pipeline S3 → Redshift para data: {date_str}")

    txs_df, dec_df = load_raw(spark, date_str)
    fact_df = transform_staging(spark, txs_df, dec_df)
    build_mart(spark, fact_df)

    logger.info("✅ Pipeline S3 → Redshift concluído")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Data de processamento (YYYY-MM-DD). Default: ontem.",
    )
    args = parser.parse_args()
    main(args.date)
