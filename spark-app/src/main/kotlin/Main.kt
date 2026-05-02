package lab2.lab

fun main() {
    val postgresConfig = PostgresConfigLoader.load()
    val clickHouseConfig = ClickHouseConfigLoader.load()
    val transformer = SnowflakeDataTransformer(postgresConfig)
    val reportLoader = ClickHouseReportLoader(postgresConfig, clickHouseConfig)

    transformer.loadSnowflakeTables()
    reportLoader.loadReports()
}
