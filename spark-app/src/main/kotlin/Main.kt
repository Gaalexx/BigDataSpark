package lab2.lab

import org.apache.spark.sql.SparkSession

fun main() {
    val spark = SparkSession.builder()
        .appName("BigDataSparkLab")
        .getOrCreate()

    val df = spark.read()
        .format("jdbc")
        .option("url", "jdbc:postgresql://db:5432/bigdata_lab")
        .option("dbtable", "mock_data")
        .option("user", "postgres")
        .option("password", "postgres")
        .option("driver", "org.postgresql.Driver")
        .load()

    df.show(10, false)

    spark.stop()
}