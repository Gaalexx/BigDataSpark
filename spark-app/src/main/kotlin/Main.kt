package lab2.lab

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.trim
import java.util.Properties


class SnowflakeDataTransformer(
    private val config: PostgresConfig
) {
    private val pgUrl = "jdbc:postgresql://${config.host}:${config.port}/${config.db}"

    private val pgProps = config.toJdbcProperties()

    private val spark: SparkSession = SparkSession.builder()
        .appName("BigDataSparkLab")
        .getOrCreate()

    private val df: Dataset<Row> = spark.read()
        .format("jdbc")
        .option("url", pgUrl)
        .option("dbtable", config.db)
        .option("user", config.user)
        .option("password", config.password)
        .option("driver", "org.postgresql.Driver")
        .load()

    fun loadDimTables() {
        loadDimSellerTable()
        spark.read()
        // loadDimCustomerTable()
        // loadDimProductTable()
        // loadDimStoreTable()
        // loadDimDateTable()
    }

    private fun loadDimSellerTable() {
        val dimSeller = df
            .select(
                trim(col("seller_email")).alias("seller_email"),
                trim(col("first_name")).alias("first_name"),
                trim(col("last_name")).alias("last_name"),
                trim(col("country")).alias("country"),
                trim(col("postal_code")).alias("postal_code")
            )
            .filter(col("seller_email").isNotNull)
            .dropDuplicates("seller_email")

        dimSeller.write()
            .mode("append")
            .jdbc(pgUrl, "dim_seller", pgProps)
    }
}

fun main() {
    val transformer = SnowflakeDataTransformer(PostgresConfigLoader.load(""))

    transformer.loadDimTables()

}