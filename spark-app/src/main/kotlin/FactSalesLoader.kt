package lab2.lab

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.trim

internal class FactSalesLoader(
    private val context: PostgresSparkContext
) {
    fun load() {
        val raw = context.rawData
            .alias("r")

        val customerKeys = context.readTable("dim_customer")
            .select(col("customer_key"), trim(col("customer_email")).alias("customer_email"))
            .alias("c")
        val sellerKeys = context.readTable("dim_seller")
            .select(col("seller_key"), trim(col("seller_email")).alias("seller_email"))
            .alias("s")
        val storeKeys = context.readTable("dim_store")
            .select(col("store_key"), trim(col("store_email")).alias("store_email"))
            .alias("st")
        val supplierKeys = context.readTable("dim_supplier")
            .select(col("supplier_key"), trim(col("supplier_email")).alias("supplier_email"))
            .alias("sup")
        val productKeys = context.readTable("dim_product")
            .select(
                col("product_key"),
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
            .alias("p")

        val factSales = raw
            .join(customerKeys, trim(col("r.customer_email")).equalTo(col("c.customer_email")), "inner")
            .join(sellerKeys, trim(col("r.seller_email")).equalTo(col("s.seller_email")), "inner")
            .join(storeKeys, trim(col("r.store_email")).equalTo(col("st.store_email")), "inner")
            .join(supplierKeys, trim(col("r.supplier_email")).equalTo(col("sup.supplier_email")), "inner")
            .join(productKeys, productJoinCondition(), "inner")
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
            .jdbc(context.jdbcUrl, "fact_sales", context.jdbcProperties)
    }

    private fun productJoinCondition(): Column =
        col("p.product_name").eqNullSafe(trim(col("r.product_name")))
            .and(col("p.product_category").eqNullSafe(trim(col("r.product_category"))))
            .and(col("p.pet_category").eqNullSafe(trim(col("r.pet_category"))))
            .and(col("p.unit_price").eqNullSafe(col("r.product_price")))
            .and(col("p.available_quantity").eqNullSafe(col("r.product_quantity")))
            .and(col("p.weight").eqNullSafe(col("r.product_weight")))
            .and(col("p.color").eqNullSafe(trim(col("r.product_color"))))
            .and(col("p.size").eqNullSafe(trim(col("r.product_size"))))
            .and(col("p.brand").eqNullSafe(trim(col("r.product_brand"))))
            .and(col("p.material").eqNullSafe(trim(col("r.product_material"))))
            .and(col("p.description").eqNullSafe(trim(col("r.product_description"))))
            .and(col("p.rating").eqNullSafe(col("r.product_rating")))
            .and(col("p.reviews").eqNullSafe(col("r.product_reviews")))
            .and(col("p.release_date").eqNullSafe(col("r.product_release_date")))
            .and(col("p.expiry_date").eqNullSafe(col("r.product_expiry_date")))
}
