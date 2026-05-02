package lab2.lab

internal class ClickHouseSchemaManager(
    private val config: ClickHouseConfig,
    private val clickHouseHttpClient: ClickHouseHttpClient
) {
    fun truncateReportTables() {
        reportTableNames().forEach { tableName ->
            clickHouseHttpClient.execute("TRUNCATE TABLE IF EXISTS $tableName", config.db)
        }
    }

    private fun reportTableNames(): List<String> = listOf(
        "report_product_sales",
        "report_customer_sales",
        "report_time_sales",
        "report_store_sales",
        "report_supplier_sales",
        "report_product_quality"
    )
}
