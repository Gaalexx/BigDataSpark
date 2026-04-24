package lab2.lab

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.trim

internal class DimensionTableLoader(
    private val context: PostgresSparkContext
) {
    private val rawData = context.rawData

    fun loadAll() {
        loadDimSellerTable()
        loadDimCustomerTable()
        loadDimProductTable()
        loadDimStoreTable()
        loadDimSupplierTable()
    }

    private fun loadDimSellerTable() {
        val dimSeller = rawData
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
            .jdbc(context.jdbcUrl, "dim_seller", context.jdbcProperties)
    }

    private fun loadDimCustomerTable() {
        val dimCustomer = rawData
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
            .jdbc(context.jdbcUrl, "dim_customer", context.jdbcProperties)
    }

    private fun loadDimProductTable() {
        val dimProduct = rawData
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
            .jdbc(context.jdbcUrl, "dim_product", context.jdbcProperties)
    }

    private fun loadDimStoreTable() {
        val dimStore = rawData
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
            .jdbc(context.jdbcUrl, "dim_store", context.jdbcProperties)
    }

    private fun loadDimSupplierTable() {
        val dimSupplier = rawData
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
            .jdbc(context.jdbcUrl, "dim_supplier", context.jdbcProperties)
    }
}
