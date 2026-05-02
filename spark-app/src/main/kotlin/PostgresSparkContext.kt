package lab2.lab

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

internal class PostgresSparkContext(
    private val config: PostgresConfig
) {
    val jdbcUrl: String = config.jdbcUrl
    val jdbcProperties = config.toJdbcProperties()

    private val spark: SparkSession = SparkSession.builder()
        .appName("BigDataSparkLab")
        .getOrCreate()

    val rawData: Dataset<Row> by lazy {
        spark.read()
            .format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", config.table)
            .option("user", config.user)
            .option("password", config.password)
            .option("driver", "org.postgresql.Driver")
            .load()
    }

    fun readTable(tableName: String): Dataset<Row> =
        spark.read()
            .format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", tableName)
            .option("user", config.user)
            .option("password", config.password)
            .option("driver", "org.postgresql.Driver")
            .load()
}
