package lab2.lab

class SnowflakeDataTransformer(
    config: PostgresConfig
) {
    private val context = PostgresSparkContext(config)
    private val dimensionTableLoader = DimensionTableLoader(context)
    private val factSalesLoader = FactSalesLoader(context)

    fun loadSnowflakeTables() {
        if (isStarSchemaAlreadyLoaded()) {
            println("PostgreSQL star schema already contains data. Skipping reload.")
            return
        }

        dimensionTableLoader.loadAll()
        factSalesLoader.load()
    }

    private fun isStarSchemaAlreadyLoaded(): Boolean =
        listOf(
            "dim_customer",
            "dim_seller",
            "dim_store",
            "dim_supplier",
            "dim_product",
            "fact_sales"
        ).any { tableName ->
            context.readTable(tableName).limit(1).count() > 0
        }
}
