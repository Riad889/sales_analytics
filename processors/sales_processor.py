from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class SalesProcessor:
    def __init__(self, spark):
        self.spark = spark

    def process_sales(self, sales_data: DataFrame) -> DataFrame:
        """Process raw sales data through transformation pipeline"""
        df = self._process_customer_data(df=sales_data)
        df = self._process_product_data(df=df)
        df = self._process_channel_data(df=df)
        df = self._process_pricing_data(df=df)
        df = self._process_payment_data(df=df)
        df = self._process_dates(df=df)
        return df

    def _process_customer_data(self, df: DataFrame) -> DataFrame:
        """Extract and flatten customer information"""
        return (
            df.withColumn(
                "customer_id",
                F.when(
                    F.col("customer.customer_id").isNotNull(),
                    F.col("customer.customer_id"),
                ).otherwise(F.lit(None)),
            )
            .withColumn(
                "customer_email",
                F.when(
                    F.col("customer.email").isNotNull(), F.col("customer.email")
                ).otherwise(F.lit(None)),
            )
            .withColumn(
                "customer_country",
                F.when(
                    F.col("customer.country").isNotNull(), F.col("customer.country")
                ).otherwise(F.lit(None)),
            )
            .withColumn(
                "customer_city",
                F.when(
                    F.col("customer.city").isNotNull(), F.col("customer.city")
                ).otherwise(F.lit(None)),
            )
            .withColumn(
                "customer_segment",
                F.when(
                    F.col("customer.segment").isNotNull(), F.col("customer.segment")
                ).otherwise(F.lit("Unknown")),
            )
            .withColumn(
                "is_returning_customer",
                F.when(
                    F.col("customer.is_returning_customer").isNotNull(),
                    F.col("customer.is_returning_customer"),
                ).otherwise(F.lit(False)),
            )
        )

    def _process_product_data(self, df: DataFrame) -> DataFrame:
        """Extract and flatten product information"""
        return (
            df.withColumn(
                "product_id",
                F.when(
                    F.col("product.product_id").isNotNull(),
                    F.col("product.product_id"),
                ).otherwise(F.lit(None)),
            )
            .withColumn(
                "product_name",
                F.when(
                    F.col("product.name").isNotNull(), F.col("product.name")
                ).otherwise(F.lit(None)),
            )
            .withColumn(
                "product_category",
                F.when(
                    F.col("product.category").isNotNull(), F.col("product.category")
                ).otherwise(F.lit(None)),
            )
            .withColumn(
                "base_price",
                F.when(
                    F.col("product.base_price").isNotNull(),
                    F.col("product.base_price"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "quantity",
                F.when(
                    F.col("product.quantity").isNotNull(), F.col("product.quantity")
                ).otherwise(F.lit(0)),
            )
        )

    def _process_channel_data(self, df: DataFrame) -> DataFrame:
        """Extract sales and marketing channel information"""
        return (
            df.withColumn(
                "sales_channel",
                F.when(
                    F.col("channel.sales_channel").isNotNull(),
                    F.col("channel.sales_channel"),
                ).otherwise(F.lit("Unknown")),
            )
            .withColumn(
                "campaign",
                F.when(
                    F.col("channel.campaign").isNotNull(), F.col("channel.campaign")
                ).otherwise(F.lit("Other")),
            )
            .withColumn(
                "traffic_source",
                F.when(
                    F.col("channel.traffic_source").isNotNull(),
                    F.col("channel.traffic_source"),
                ).otherwise(F.lit("Direct")),
            )
            .withColumn(
                "device_type",
                F.when(
                    F.col("channel.device").isNotNull(), F.col("channel.device")
                ).otherwise(F.lit("Unknown")),
            )
        )

    def _process_pricing_data(self, df: DataFrame) -> DataFrame:
        """Extract and calculate pricing metrics"""
        return (
            df.withColumn(
                "unit_price",
                F.when(
                    F.col("pricing.unit_price").isNotNull(),
                    F.col("pricing.unit_price"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "subtotal",
                F.when(
                    F.col("pricing.subtotal").isNotNull(),
                    F.col("pricing.subtotal"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "discount_rate",
                F.when(
                    F.col("pricing.discount_rate").isNotNull(),
                    F.col("pricing.discount_rate"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "discount_amount",
                F.when(
                    F.col("pricing.discount_amount").isNotNull(),
                    F.col("pricing.discount_amount"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "tax_amount",
                F.when(
                    F.col("pricing.tax_amount").isNotNull(),
                    F.col("pricing.tax_amount"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "shipping_fee",
                F.when(
                    F.col("pricing.shipping_fee").isNotNull(),
                    F.col("pricing.shipping_fee"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "total_amount",
                F.when(
                    F.col("pricing.total_amount").isNotNull(),
                    F.col("pricing.total_amount"),
                ).otherwise(F.lit(0.0)),
            )
        )

    def _process_payment_data(self, df: DataFrame) -> DataFrame:
        """Extract payment method and status"""
        return df.withColumn(
            "payment_method",
            F.when(
                F.col("payment.payment_method").isNotNull(),
                F.col("payment.payment_method"),
            ).otherwise(F.lit("Unknown")),
        ).withColumn(
            "payment_status",
            F.when(
                F.col("payment.payment_status").isNotNull(),
                F.col("payment.payment_status"),
            ).otherwise(F.lit("Unknown")),
        )

    def _process_dates(self, df: DataFrame) -> DataFrame:
        """Process and standardize date fields"""
        return (
            df.withColumn(
                "order_date_ts",
                F.to_timestamp(
                    F.when(
                        F.col("order_date").isNotNull(), F.col("order_date")
                    ).otherwise(None)
                ),
            )
            .withColumn(
                "confirmation_date_ts",
                F.to_timestamp(
                    F.when(
                        F.col("confirmation_date").isNotNull(),
                        F.col("confirmation_date"),
                    ).otherwise(None)
                ),
            )
            .withColumn(
                "order_status",
                F.when(
                    F.col("order_status").isNotNull(), F.col("order_status")
                ).otherwise(F.lit("Unknown")),
            )
        )
