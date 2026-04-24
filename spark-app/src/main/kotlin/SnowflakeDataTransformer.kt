package lab2.lab

class SnowflakeDataTransformer(
    config: PostgresConfig
) {
    private val context = PostgresSparkContext(config)
    private val dimensionTableLoader = DimensionTableLoader(context)
    private val factSalesLoader = FactSalesLoader(context)

    fun loadSnowflakeTables() {
        dimensionTableLoader.loadAll()
        factSalesLoader.load()
    }
}
