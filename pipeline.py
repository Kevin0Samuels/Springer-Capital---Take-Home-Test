import glob
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder \
        .appName("SpringerCapital-ReferralFraudDetection") \
        .getOrCreate()

    data_dir = os.getenv("DATA_DIR", "/app/data")
    
    df_lead_logs = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_dir}/lead_logs.csv")
    df_paid_transactions = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_dir}/paid_transactions.csv")
    df_referral_rewards = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_dir}/referral_rewards.csv")
    df_user_logs = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_dir}/user_logs.csv")
    df_user_referral_logs = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_dir}/user_referral_logs.csv")
    df_user_referral_statuses = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_dir}/user_referral_statuses.csv")
    df_user_referrals = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_dir}/user_referrals.csv")

    df_user_referrals_clean = df_user_referrals.dropna(subset=["referral_id", "referrer_id", "user_referral_status_id"])

    df_referrer = df_user_logs.alias("ref_user")
    df_referee = df_user_logs.alias("referee_user")

    pipeline_df = df_user_referrals_clean.alias("ur") \
        .join(df_user_referral_statuses.alias("urs"), F.col("ur.user_referral_status_id") == F.col("urs.Id"), "left") \
        .join(df_referral_rewards.alias("rr"), F.col("ur.referral_reward_id") == F.col("rr.id"), "left") \
        .join(df_user_referral_logs.alias("url"), F.col("ur.referral_id") == F.col("url.user_referral_id"), "left") \
        .join(df_paid_transactions.alias("pt"), F.col("ur.transaction_id") == F.col("pt.transaction_id"), "left") \
        .join(df_lead_logs.alias("ll"), (F.col("ur.referral_source") == "Lead") & (F.col("ur.referee_id") == F.col("ll.lead_id")), "left") \
        .join(df_referrer, F.col("ur.referrer_id") == F.col("ref_user.user_id"), "left") \
        .join(df_referee, F.col("ur.referee_id") == F.col("referee_user.user_id"), "left")

    pipeline_df = pipeline_df.withColumn(
        "referral_source_category",
        F.when(F.col("ur.referral_source") == "User Sign Up", "Online")
         .when(F.col("ur.referral_source") == "Draft Transaction", "Offline")
         .when(F.col("ur.referral_source") == "Lead", F.col("ll.source_category"))
         .otherwise(None)
    )

    pipeline_df = pipeline_df \
        .withColumn("referrer_name_clean", F.initcap(F.col("ref_user.name"))) \
        .withColumn("referee_name_clean", F.initcap(F.col("ur.referee_name"))) \
        .withColumn("referrer_homeclub", F.col("ref_user.homeclub"))

    is_valid_expr = (
        (
            (F.col("rr.reward_value") > 0) &
            (F.col("urs.description") == "Berhasil") &
            (F.col("ur.transaction_id").isNotNull()) &
            (F.upper(F.col("pt.transaction_status")) == "PAID") &
            (F.upper(F.col("pt.transaction_type")) == "NEW") &
            (F.col("pt.transaction_at") > F.col("ur.referral_at")) &
            (F.date_format(F.col("pt.transaction_at"), "yyyy-MM") == F.date_format(F.col("ur.referral_at"), "yyyy-MM")) &
            (F.col("ref_user.membership_expired_date") > F.col("ur.referral_at")) &
            (F.col("ref_user.is_deleted") == False) &
            (F.col("url.is_reward_granted") == True)
        ) |
        (
            (F.col("urs.description").isin("Menunggu", "Tidak Berhasil")) &
            (F.col("rr.reward_value").isNull() | (F.col("rr.reward_value") == 0))
        )
    )

    is_invalid_expr = (
        ((F.col("rr.reward_value") > 0) & (F.col("urs.description") != "Berhasil")) |
        ((F.col("rr.reward_value") > 0) & (F.col("ur.transaction_id").isNull())) |
        ((F.col("rr.reward_value").isNull() | (F.col("rr.reward_value") == 0)) & 
         (F.col("ur.transaction_id").isNotNull()) & 
         (F.upper(F.col("pt.transaction_status")) == "PAID") & 
         (F.col("pt.transaction_at") > F.col("ur.referral_at"))) |
        ((F.col("urs.description") == "Berhasil") & (F.col("rr.reward_value").isNull() | (F.col("rr.reward_value") == 0))) |
        (F.col("pt.transaction_at") < F.col("ur.referral_at"))
    )

    pipeline_df = pipeline_df.withColumn(
        "is_business_logic_valid",
        F.when(is_invalid_expr, False).otherwise(is_valid_expr)
    )

    output_df = pipeline_df.select(
        F.col("url.id").alias("referral_details_id"),
        F.col("ur.referral_id"),
        F.col("ur.referral_source"),
        F.col("referral_source_category"),
        F.col("ur.referral_at"),
        F.col("ur.referrer_id"),
        F.col("referrer_name_clean").alias("referrer_name"),
        F.col("ref_user.phone_number").alias("referrer_phone_number"),
        F.col("referrer_homeclub"),
        F.col("ur.referee_id"),
        F.col("referee_name_clean").alias("referee_name"),
        F.col("ur.referee_phone"),
        F.col("urs.description").alias("referral_status"),
        F.lit(30).alias("num_reward_days"),
        F.col("ur.transaction_id"),
        F.col("pt.transaction_status"),
        F.col("pt.transaction_at"),
        F.col("pt.transaction_location"),
        F.col("pt.transaction_type"),
        F.col("ur.updated_at"),
        F.col("url.created_at").alias("reward_granted_at"),
        F.col("is_business_logic_valid")
    ).dropDuplicates(["referral_id"])

    output_file = os.getenv("OUTPUT_FILE", "/app/output/result_data.csv")
    output_dir = os.path.dirname(output_file) or "."
    temp_output_path = os.path.join(output_dir, ".result_data_spark_tmp")

    os.makedirs(output_dir, exist_ok=True)
    if os.path.exists(temp_output_path):
        shutil.rmtree(temp_output_path)
    if os.path.exists(output_file):
        os.remove(output_file)

    output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_path)

    part_files = glob.glob(os.path.join(temp_output_path, "part-*.csv"))
    if len(part_files) != 1:
        raise RuntimeError(f"Expected one Spark CSV part file, found {len(part_files)}")

    os.replace(part_files[0], output_file)
    shutil.rmtree(temp_output_path)
    print(f"Wrote {output_df.count()} rows to {output_file}")

    spark.stop()

if __name__ == "__main__":
    main()
