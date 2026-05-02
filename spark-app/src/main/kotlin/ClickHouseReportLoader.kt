package lab2.lab

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.corr
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.functions.lag
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.month
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions.trunc
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.functions.`when`

internal class ClickHouseReportLoader(
    postgresConfig: PostgresConfig,
    clickHouseConfig: ClickHouseConfig
) {
    private val postgresContext = PostgresSparkContext(postgresConfig)
    private val clickHouseHttpClient = ClickHouseHttpClient(clickHouseConfig)
    private val schemaManager = ClickHouseSchemaManager(clickHouseConfig, clickHouseHttpClient)

    fun loadReports() {
        schemaManager.truncateReportTables()

        val salesBase = buildSalesBase().cache()

        writeReport("report_product_sales", buildProductSalesReport(salesBase))
        writeReport("report_customer_sales", buildCustomerSalesReport(salesBase))
        writeReport("report_time_sales", buildTimeSalesReport(salesBase))
        writeReport("report_store_sales", buildStoreSalesReport(salesBase))
        writeReport("report_supplier_sales", buildSupplierSalesReport(salesBase))
        writeReport("report_product_quality", buildProductQualityReport(salesBase))

        salesBase.unpersist()
    }

    private fun buildSalesBase(): Dataset<Row> =
        postgresContext.readTable("fact_sales")
            .dropDuplicates("source_raw_key")
            .alias("f")
            .join(
                postgresContext.readTable("dim_customer").alias("c"),
                col("f.customer_key").equalTo(col("c.customer_key")),
                "inner"
            )
            .join(
                postgresContext.readTable("dim_store").alias("st"),
                col("f.store_key").equalTo(col("st.store_key")),
                "inner"
            )
            .join(
                postgresContext.readTable("dim_supplier").alias("sup"),
                col("f.supplier_key").equalTo(col("sup.supplier_key")),
                "inner"
            )
            .join(
                postgresContext.readTable("dim_product").alias("p"),
                col("f.product_key").equalTo(col("p.product_key")),
                "inner"
            )
            .select(
                col("f.source_raw_key").cast("string").alias("source_raw_key"),
                col("f.sale_date").alias("sale_date"),
                col("f.sale_quantity").cast("long").alias("sale_quantity"),
                col("f.sale_total_price").cast("double").alias("sale_total_price"),
                col("f.source_unit_price").cast("double").alias("source_unit_price"),
                trim(col("c.customer_email")).alias("customer_email"),
                trim(col("c.first_name")).alias("customer_first_name"),
                trim(col("c.last_name")).alias("customer_last_name"),
                trim(col("c.country")).alias("customer_country"),
                trim(col("c.postal_code")).alias("customer_postal_code"),
                trim(col("c.pet_type")).alias("customer_pet_type"),
                trim(col("c.pet_breed")).alias("customer_pet_breed"),
                trim(col("st.store_email")).alias("store_email"),
                trim(col("st.store_name")).alias("store_name"),
                trim(col("st.location")).alias("store_location"),
                trim(col("st.city")).alias("store_city"),
                trim(col("st.state")).alias("store_state"),
                trim(col("st.country")).alias("store_country"),
                trim(col("sup.supplier_email")).alias("supplier_email"),
                trim(col("sup.supplier_name")).alias("supplier_name"),
                trim(col("sup.contact_name")).alias("supplier_contact_name"),
                trim(col("sup.city")).alias("supplier_city"),
                trim(col("sup.country")).alias("supplier_country"),
                trim(col("p.product_name")).alias("product_name"),
                trim(col("p.product_category")).alias("product_category"),
                trim(col("p.pet_category")).alias("pet_category"),
                trim(col("p.brand")).alias("product_brand"),
                trim(col("p.color")).alias("product_color"),
                trim(col("p.size")).alias("product_size"),
                trim(col("p.material")).alias("product_material"),
                col("p.unit_price").cast("double").alias("product_unit_price"),
                col("p.rating").cast("double").alias("product_rating"),
                col("p.reviews").cast("int").alias("product_reviews")
            )

    private fun buildProductSalesReport(salesBase: Dataset<Row>): Dataset<Row> {
        val aggregated = salesBase
            .groupBy(
                col("product_name"),
                col("product_category"),
                col("pet_category"),
                col("product_brand"),
                col("product_color"),
                col("product_size"),
                col("product_material"),
                col("product_unit_price")
            )
            .agg(
                round(avg(col("product_rating")), 2).alias("avg_rating"),
                max(col("product_reviews")).alias("reviews_count"),
                round(sum(col("sale_total_price")), 2).alias("total_revenue"),
                sum(col("sale_quantity")).cast("long").alias("total_quantity_sold"),
                count(col("source_raw_key")).cast("long").alias("total_sales_count")
            )

        val revenueWindow = Window.orderBy(col("total_revenue").desc(), col("product_name").asc())
        val quantityWindow = Window.orderBy(col("total_quantity_sold").desc(), col("product_name").asc())

        return aggregated
            .withColumn("revenue_rank", dense_rank().over(revenueWindow).cast("int"))
            .withColumn("quantity_rank", dense_rank().over(quantityWindow).cast("int"))
            .select(
                col("product_name"),
                col("product_category"),
                col("pet_category"),
                col("product_brand").alias("brand"),
                col("product_color").alias("color"),
                col("product_size").alias("size"),
                col("product_material").alias("material"),
                col("product_unit_price").alias("unit_price"),
                col("avg_rating"),
                col("reviews_count"),
                col("total_revenue"),
                col("total_quantity_sold"),
                col("total_sales_count"),
                col("revenue_rank"),
                col("quantity_rank")
            )
    }

    private fun buildCustomerSalesReport(salesBase: Dataset<Row>): Dataset<Row> {
        val aggregated = salesBase
            .groupBy(
                col("customer_email"),
                col("customer_first_name"),
                col("customer_last_name"),
                col("customer_country"),
                col("customer_postal_code"),
                col("customer_pet_type"),
                col("customer_pet_breed")
            )
            .agg(
                round(sum(col("sale_total_price")), 2).alias("total_purchase_amount"),
                count(col("source_raw_key")).cast("long").alias("total_orders_count"),
                sum(col("sale_quantity")).cast("long").alias("total_quantity_purchased"),
                round(avg(col("sale_total_price")), 2).alias("avg_check")
            )

        val spendWindow = Window.orderBy(col("total_purchase_amount").desc(), col("customer_email").asc())

        return aggregated
            .withColumn("spend_rank", dense_rank().over(spendWindow).cast("int"))
            .select(
                col("customer_email"),
                col("customer_first_name").alias("first_name"),
                col("customer_last_name").alias("last_name"),
                col("customer_country").alias("country"),
                col("customer_postal_code").alias("postal_code"),
                col("customer_pet_type").alias("pet_type"),
                col("customer_pet_breed").alias("pet_breed"),
                col("total_purchase_amount"),
                col("total_orders_count"),
                col("total_quantity_purchased"),
                col("avg_check"),
                col("spend_rank")
            )
    }

    private fun buildTimeSalesReport(salesBase: Dataset<Row>): Dataset<Row> {
        val monthlySales = salesBase
            .withColumn("sale_year", year(col("sale_date")).cast("int"))
            .withColumn("sale_month", month(col("sale_date")).cast("int"))
            .withColumn("month_start", trunc(col("sale_date"), "month"))
            .groupBy(
                col("sale_year"),
                col("sale_month"),
                col("month_start")
            )
            .agg(
                round(sum(col("sale_total_price")), 2).alias("total_revenue"),
                count(col("source_raw_key")).cast("long").alias("total_orders_count"),
                sum(col("sale_quantity")).cast("long").alias("total_quantity_sold"),
                round(avg(col("sale_total_price")), 2).alias("avg_order_size")
            )

        val chronologicalWindow = Window.orderBy(col("month_start").asc())
        val sameMonthWindow = Window.partitionBy("sale_month").orderBy(col("sale_year").asc())
        val yearWindow = Window.partitionBy("sale_year")

        return monthlySales
            .withColumn("prev_month_revenue", lag(col("total_revenue"), 1).over(chronologicalWindow))
            .withColumn("prev_year_same_month_revenue", lag(col("total_revenue"), 1).over(sameMonthWindow))
            .withColumn(
                "revenue_change_vs_prev_month",
                `when`(col("prev_month_revenue").isNull, lit(null))
                    .otherwise(round(col("total_revenue") - col("prev_month_revenue"), 2))
            )
            .withColumn(
                "revenue_change_vs_prev_year_same_month",
                `when`(col("prev_year_same_month_revenue").isNull, lit(null))
                    .otherwise(round(col("total_revenue") - col("prev_year_same_month_revenue"), 2))
            )
            .withColumn("year_total_revenue", round(sum(col("total_revenue")).over(yearWindow), 2))
            .select(
                col("sale_year"),
                col("sale_month"),
                col("month_start"),
                col("total_revenue"),
                col("total_orders_count"),
                col("total_quantity_sold"),
                col("avg_order_size"),
                col("prev_month_revenue"),
                col("prev_year_same_month_revenue"),
                col("revenue_change_vs_prev_month"),
                col("revenue_change_vs_prev_year_same_month"),
                col("year_total_revenue")
            )
    }

    private fun buildStoreSalesReport(salesBase: Dataset<Row>): Dataset<Row> {
        val aggregated = salesBase
            .groupBy(
                col("store_email"),
                col("store_name"),
                col("store_location"),
                col("store_city"),
                col("store_state"),
                col("store_country")
            )
            .agg(
                round(sum(col("sale_total_price")), 2).alias("total_revenue"),
                count(col("source_raw_key")).cast("long").alias("total_orders_count"),
                sum(col("sale_quantity")).cast("long").alias("total_quantity_sold"),
                round(avg(col("sale_total_price")), 2).alias("avg_check")
            )

        val revenueWindow = Window.orderBy(col("total_revenue").desc(), col("store_email").asc())

        return aggregated
            .withColumn("revenue_rank", dense_rank().over(revenueWindow).cast("int"))
            .select(
                col("store_email"),
                col("store_name"),
                col("store_location").alias("location"),
                col("store_city").alias("city"),
                col("store_state").alias("state"),
                col("store_country").alias("country"),
                col("total_revenue"),
                col("total_orders_count"),
                col("total_quantity_sold"),
                col("avg_check"),
                col("revenue_rank")
            )
    }

    private fun buildSupplierSalesReport(salesBase: Dataset<Row>): Dataset<Row> {
        val aggregated = salesBase
            .groupBy(
                col("supplier_email"),
                col("supplier_name"),
                col("supplier_contact_name"),
                col("supplier_city"),
                col("supplier_country")
            )
            .agg(
                round(avg(col("source_unit_price")), 2).alias("avg_product_price"),
                round(sum(col("sale_total_price")), 2).alias("total_revenue"),
                count(col("source_raw_key")).cast("long").alias("total_orders_count"),
                sum(col("sale_quantity")).cast("long").alias("total_quantity_sold")
            )

        val revenueWindow = Window.orderBy(col("total_revenue").desc(), col("supplier_email").asc())

        return aggregated
            .withColumn("revenue_rank", dense_rank().over(revenueWindow).cast("int"))
            .select(
                col("supplier_email"),
                col("supplier_name"),
                col("supplier_contact_name").alias("contact_name"),
                col("supplier_city").alias("city"),
                col("supplier_country").alias("country"),
                col("avg_product_price"),
                col("total_revenue"),
                col("total_orders_count"),
                col("total_quantity_sold"),
                col("revenue_rank")
            )
    }

    private fun buildProductQualityReport(salesBase: Dataset<Row>): Dataset<Row> {
        val aggregated = salesBase
            .groupBy(
                col("product_name"),
                col("product_category"),
                col("pet_category"),
                col("product_brand"),
                col("product_color"),
                col("product_size"),
                col("product_material"),
                col("product_rating"),
                col("product_reviews")
            )
            .agg(
                sum(col("sale_quantity")).cast("long").alias("total_quantity_sold"),
                round(sum(col("sale_total_price")), 2).alias("total_revenue")
            )

        val correlation = aggregated
            .agg(corr(col("product_rating"), col("total_quantity_sold")).alias("rating_sales_correlation"))

        val ratingDescWindow = Window.orderBy(col("product_rating").desc(), col("product_name").asc())
        val ratingAscWindow = Window.orderBy(col("product_rating").asc(), col("product_name").asc())
        val reviewsWindow = Window.orderBy(col("product_reviews").desc(), col("product_name").asc())
        val salesWindow = Window.orderBy(col("total_quantity_sold").desc(), col("product_name").asc())

        return aggregated
            .crossJoin(correlation)
            .withColumn("rating_rank_desc", dense_rank().over(ratingDescWindow).cast("int"))
            .withColumn("rating_rank_asc", dense_rank().over(ratingAscWindow).cast("int"))
            .withColumn("reviews_rank", dense_rank().over(reviewsWindow).cast("int"))
            .withColumn("sales_rank", dense_rank().over(salesWindow).cast("int"))
            .select(
                col("product_name"),
                col("product_category"),
                col("pet_category"),
                col("product_brand").alias("brand"),
                col("product_color").alias("color"),
                col("product_size").alias("size"),
                col("product_material").alias("material"),
                col("product_rating").alias("rating"),
                col("product_reviews").alias("reviews_count"),
                col("total_quantity_sold"),
                col("total_revenue"),
                col("rating_rank_desc"),
                col("rating_rank_asc"),
                col("reviews_rank"),
                col("sales_rank"),
                col("rating_sales_correlation")
            )
    }

    private fun writeReport(tableName: String, report: Dataset<Row>) {
        clickHouseHttpClient.insertJsonEachRow(tableName, report.toJSON().collectAsList())
    }
}
