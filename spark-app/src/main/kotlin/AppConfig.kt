package lab2.lab

import java.util.Properties

data class PostgresConfig(
    val host: String,
    val port: String,
    val db: String,
    val table: String,
    val user: String,
    val password: String
) {
    val jdbcUrl: String
        get() = "jdbc:postgresql://$host:$port/$db"
}

data class ClickHouseConfig(
    val host: String,
    val port: String,
    val db: String,
    val user: String,
    val password: String
) {
    val adminJdbcUrl: String
        get() = "jdbc:clickhouse://$host:$port/default"

    val jdbcUrl: String
        get() = "jdbc:clickhouse://$host:$port/$db"
}

fun PostgresConfig.toJdbcProperties(): Properties =
    Properties().apply {
        put("user", user)
        put("password", password)
        put("driver", "org.postgresql.Driver")
        put("stringtype", "unspecified")
    }

fun ClickHouseConfig.toJdbcProperties(): Properties =
    Properties().apply {
        put("user", user)
        put("password", password)
        put("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    }

private fun env(name: String, default: String): String =
    System.getenv(name) ?: default

object PostgresConfigLoader {
    fun load(tableName: String = env("POSTGRES_TABLE", "mock_data_raw")) : PostgresConfig = PostgresConfig(
        host = env("POSTGRES_HOST", "db"),
        port = env("POSTGRES_PORT", "5432"),
        db = env("POSTGRES_DB", "lab2_db"),
        table = tableName,
        user = env("POSTGRES_USER", "lab_user"),
        password = env("POSTGRES_PASSWORD", "lab_password")
    )
}

object ClickHouseConfigLoader {
    fun load() : ClickHouseConfig = ClickHouseConfig(
        host = env("CLICKHOUSE_HOST", "clickhouse"),
        port = env("CLICKHOUSE_PORT", "8123"),
        db = env("CLICKHOUSE_DB", "lab2_marts"),
        user = env("CLICKHOUSE_USER", "default"),
        password = env("CLICKHOUSE_PASSWORD", "clickhouse")
    )
}
