package lab2.lab

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sha2
import org.apache.spark.sql.functions.trim
import java.sql.DriverManager

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
        .option("dbtable", config.table)
        .option("user", config.user)
        .option("password", config.password)
        .option("driver", "org.postgresql.Driver")
        .load()

    fun loadSnowflakeTables() {
        resetTargetTables()
        loadDimTables()
        loadFactSalesTable()
    }

    private fun resetTargetTables() {
        DriverManager.getConnection(pgUrl, pgProps).use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                    """
                    TRUNCATE TABLE
                        fact_sales,
                        dim_customer,
                        dim_seller,
                        dim_store,
                        dim_supplier,
                        dim_product
                    RESTART IDENTITY CASCADE
                    """.trimIndent()
                )
            }
        }
    }

    private fun loadDimTables() {
        loadDimSellerTable()
        loadDimCustomerTable()
        loadDimProductTable()
        loadDimStoreTable()
        loadDimSupplierTable()
    }

    private fun loadDimSellerTable() {
        val dimSeller = df
            .select(
                trim(col("seller_email")).alias("seller_email"),
                trim(col("seller_first_name")).alias("first_name"),
                trim(col("seller_last_name")).alias("last_name"),
                trim(col("seller_country")).alias("country"),
                trim(col("seller_postal_code")).alias("postal_code")
            )
            .filter(col("seller_email").isNotNull)
            .dropDuplicates("seller_email")

        dimSeller.write()
            .mode("append")
            .jdbc(pgUrl, "dim_seller", pgProps)
    }

    private fun loadDimCustomerTable() {
        val dimCustomer = df
            .select(
                trim(col("customer_email")).alias("customer_email"),
                trim(col("customer_first_name")).alias("first_name"),
                trim(col("customer_last_name")).alias("last_name"),
                col("customer_age").alias("age"),
                trim(col("customer_country")).alias("country"),
                trim(col("customer_postal_code")).alias("postal_code"),
                trim(col("customer_pet_type")).alias("pet_type"),
                trim(col("customer_pet_name")).alias("pet_name"),
                trim(col("customer_pet_breed")).alias("pet_breed")
            )
            .filter(col("customer_email").isNotNull)
            .dropDuplicates("customer_email")

        dimCustomer.write()
            .mode("append")
            .jdbc(pgUrl, "dim_customer", pgProps)
    }

    private fun loadDimProductTable() {
        val dimProduct = df
            .select(
                trim(col("product_name")).alias("product_name"),
                trim(col("product_category")).alias("product_category"),
                trim(col("pet_category")).alias("pet_category"),
                col("product_price").alias("unit_price"),
                col("product_quantity").alias("available_quantity"),
                col("product_weight").alias("weight"),
                trim(col("product_color")).alias("color"),
                trim(col("product_size")).alias("size"),
                trim(col("product_brand")).alias("brand"),
                trim(col("product_material")).alias("material"),
                trim(col("product_description")).alias("description"),
                col("product_rating").alias("rating"),
                col("product_reviews").alias("reviews"),
                col("product_release_date").alias("release_date"),
                col("product_expiry_date").alias("expiry_date")
            )
            .dropDuplicates()

        dimProduct.write()
            .mode("append")
            .jdbc(pgUrl, "dim_product", pgProps)
    }

    private fun loadDimStoreTable() {
        val dimStore = df
            .select(
                trim(col("store_email")).alias("store_email"),
                trim(col("store_name")).alias("store_name"),
                trim(col("store_location")).alias("location"),
                trim(col("store_city")).alias("city"),
                trim(col("store_state")).alias("state"),
                trim(col("store_country")).alias("country"),
                trim(col("store_phone")).alias("phone")
            )
            .filter(col("store_email").isNotNull)
            .dropDuplicates("store_email")

        dimStore.write()
            .mode("append")
            .jdbc(pgUrl, "dim_store", pgProps)
    }

    private fun loadDimSupplierTable() {
        val dimSupplier = df
            .select(
                trim(col("supplier_email")).alias("supplier_email"),
                trim(col("supplier_name")).alias("supplier_name"),
                trim(col("supplier_contact")).alias("contact_name"),
                trim(col("supplier_phone")).alias("phone"),
                trim(col("supplier_address")).alias("address"),
                trim(col("supplier_city")).alias("city"),
                trim(col("supplier_country")).alias("country")
            )
            .filter(col("supplier_email").isNotNull)
            .dropDuplicates("supplier_email")

        dimSupplier.write()
            .mode("append")
            .jdbc(pgUrl, "dim_supplier", pgProps)
    }

    private fun loadFactSalesTable() {
        val raw = df
            .withColumn("product_join_key", rawProductJoinKey())
            .alias("r")

        val customerKeys = readTable("dim_customer")
            .select(col("customer_key"), trim(col("customer_email")).alias("customer_email"))
            .alias("c")
        val sellerKeys = readTable("dim_seller")
            .select(col("seller_key"), trim(col("seller_email")).alias("seller_email"))
            .alias("s")
        val storeKeys = readTable("dim_store")
            .select(col("store_key"), trim(col("store_email")).alias("store_email"))
            .alias("st")
        val supplierKeys = readTable("dim_supplier")
            .select(col("supplier_key"), trim(col("supplier_email")).alias("supplier_email"))
            .alias("sup")
        val productKeys = readTable("dim_product")
            .withColumn("product_join_key", dimProductJoinKey())
            .select(col("product_key"), col("product_join_key"))
            .dropDuplicates("product_join_key")
            .alias("p")

        val factSales = raw
            .join(customerKeys, trim(col("r.customer_email")).equalTo(col("c.customer_email")), "inner")
            .join(sellerKeys, trim(col("r.seller_email")).equalTo(col("s.seller_email")), "inner")
            .join(storeKeys, trim(col("r.store_email")).equalTo(col("st.store_email")), "inner")
            .join(supplierKeys, trim(col("r.supplier_email")).equalTo(col("sup.supplier_email")), "inner")
            .join(productKeys, col("r.product_join_key").equalTo(col("p.product_join_key")), "inner")
            .select(
                col("r.id").alias("source_raw_id"),
                col("r.row_key").alias("source_raw_key"),
                col("c.customer_key"),
                col("s.seller_key"),
                col("st.store_key"),
                col("sup.supplier_key"),
                col("p.product_key"),
                col("r.sale_quantity"),
                col("r.sale_total_price"),
                col("r.product_price").alias("source_unit_price"),
                col("r.product_quantity").alias("source_product_quantity"),
                col("r.sale_date")
            )
            .filter(col("source_raw_id").isNotNull)
            .filter(col("source_raw_key").isNotNull)
            .filter(col("sale_quantity").isNotNull)
            .filter(col("sale_total_price").isNotNull)
            .filter(col("sale_date").isNotNull)

        factSales.write()
            .mode("append")
            .jdbc(pgUrl, "fact_sales", pgProps)
    }

    private fun readTable(tableName: String): Dataset<Row> =
        spark.read()
            .format("jdbc")
            .option("url", pgUrl)
            .option("dbtable", tableName)
            .option("user", config.user)
            .option("password", config.password)
            .option("driver", "org.postgresql.Driver")
            .load()

    private fun rawProductJoinKey(): Column = productJoinKey(
        col("product_name"),
        col("product_category"),
        col("pet_category"),
        col("product_price"),
        col("product_quantity"),
        col("product_weight"),
        col("product_color"),
        col("product_size"),
        col("product_brand"),
        col("product_material"),
        col("product_description"),
        col("product_rating"),
        col("product_reviews"),
        col("product_release_date"),
        col("product_expiry_date")
    )

    private fun dimProductJoinKey(): Column = productJoinKey(
        col("product_name"),
        col("product_category"),
        col("pet_category"),
        col("unit_price"),
        col("available_quantity"),
        col("weight"),
        col("color"),
        col("size"),
        col("brand"),
        col("material"),
        col("description"),
        col("rating"),
        col("reviews"),
        col("release_date"),
        col("expiry_date")
    )

    private fun productJoinKey(vararg columns: Column): Column =
        sha2(
            concat_ws(
                "||",
                *columns.map { coalesce(trim(it.cast("string")), lit("__NULL__")) }.toTypedArray()
            ),
            256
        )
}

fun main() {
    val transformer = SnowflakeDataTransformer(PostgresConfigLoader.load())

    transformer.loadSnowflakeTables()
}
