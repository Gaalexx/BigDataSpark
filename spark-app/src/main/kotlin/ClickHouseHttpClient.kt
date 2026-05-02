package lab2.lab

import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration

internal class ClickHouseHttpClient(
    private val config: ClickHouseConfig
) {
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build()

    fun execute(sql: String, database: String? = null) {
        send(sql, database)
    }

    fun insertJsonEachRow(tableName: String, jsonRows: List<String>) {
        if (jsonRows.isEmpty()) {
            return
        }

        val payload = buildString {
            append("INSERT INTO $tableName FORMAT JSONEachRow\n")
            jsonRows.forEach { row ->
                append(row)
                append('\n')
            }
        }

        send(payload, config.db)
    }

    private fun send(payload: String, database: String?) {
        val request = HttpRequest.newBuilder(endpoint(database))
            .timeout(Duration.ofMinutes(2))
            .header("Content-Type", "text/plain; charset=UTF-8")
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

        if (response.statusCode() !in 200..299) {
            throw IllegalStateException(
                "ClickHouse request failed with status ${response.statusCode()}: ${response.body()}"
            )
        }
    }

    private fun endpoint(database: String?): URI {
        val queryParts = mutableListOf(
            "user=${encode(config.user)}",
            "password=${encode(config.password)}"
        )

        if (database != null) {
            queryParts += "database=${encode(database)}"
        }

        return URI.create("http://${config.host}:${config.port}/?${queryParts.joinToString("&")}")
    }

    private fun encode(value: String): String =
        URLEncoder.encode(value, StandardCharsets.UTF_8)
}
