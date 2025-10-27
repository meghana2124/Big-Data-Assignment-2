from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max as spark_max, round as spark_round, unix_timestamp, from_unixtime, expr, when, asc, to_timestamp
import os
import socket

# ================= CONFIG =================
NET_FILE = "net_data1.csv"
DISK_FILE = "disk_data1.csv"
OUTPUT_FOLDER = "team_NO_NET_DISK.csv"  # Spark creates a folder

NET_THRESHOLD = 4340.87
DISK_THRESHOLD = 3823.46
TIMESTAMP_FORMAT = "HH:mm:ss"

# Temporary Spark folder
LOCAL_TMP_DIR = os.path.expanduser("~/spark_tmp")
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
os.environ["SPARK_LOCAL_DIRS"] = LOCAL_TMP_DIR
os.environ["SPARK_LOCAL_IP"] = socket.gethostbyname(socket.gethostname())
# ==========================================

def run_spark_job_net_disk():
    
    spark = SparkSession.builder.appName("SparkJob_NET_DISK").getOrCreate()
    print(f"Loading data from {NET_FILE} and {DISK_FILE}...")

    
    net_df = spark.read.csv(NET_FILE, header=True, inferSchema=True)
    disk_df = spark.read.csv(DISK_FILE, header=True, inferSchema=True)

    
    net_df = net_df.withColumn("ts", to_timestamp(col("ts"), TIMESTAMP_FORMAT))
    disk_df = disk_df.withColumn("ts", to_timestamp(col("ts"), TIMESTAMP_FORMAT))

    
    joined_df = net_df.join(disk_df, on=["ts", "server_id"], how="inner")

    
    first_ts = joined_df.agg({"ts": "min"}).collect()[0][0]

    
    agg_df = joined_df.groupBy(
        col("server_id"),
        window(col("ts"), "30 seconds", "10 seconds").alias("win")
    ).agg(
        spark_round(spark_max("net_in"), 2).alias("max_net_in"),
        spark_round(spark_max("disk_io"), 2).alias("max_disk_io")
    )

    
    agg_df = agg_df.filter(col("win.start") >= first_ts)

    
    agg_df = agg_df.withColumn(
        "alert",
        when((col("max_net_in") > NET_THRESHOLD) & (col("max_disk_io") > DISK_THRESHOLD),
             "Network flood + Disk thrash suspected")
        .when(col("max_net_in") > NET_THRESHOLD, "Possible DDoS")
        .when(col("max_disk_io") > DISK_THRESHOLD, "Disk thrash suspected")
        .otherwise("")  # <-- blank instead of "No Alert"
    )

    
    final_df = agg_df.select(
        "server_id",
        from_unixtime(unix_timestamp(col("win.start").cast("string")), "HH:mm:ss").alias("window_start"),
        from_unixtime(unix_timestamp(col("win.end").cast("string")), "HH:mm:ss").alias("window_end"),
        "max_net_in",
        "max_disk_io",
        "alert"
    ).orderBy(asc("server_id"), asc("window_start"))

    
    final_df.coalesce(1).write.csv(OUTPUT_FOLDER, header=True, mode="overwrite")

    
    spark.stop()

    
    row_count = final_df.count()
    print(f"\n Spark Job Completed. CSV written to folder '{OUTPUT_FOLDER}'")
    print("Final row count:", row_count)

if __name__ == "__main__":
    run_spark_job_net_disk()

