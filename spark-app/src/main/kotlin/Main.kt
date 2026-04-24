package lab2.lab

fun main() {
    val transformer = SnowflakeDataTransformer(PostgresConfigLoader.load())

    transformer.loadSnowflakeTables()
}
