import random

from uuid import uuid4
from generators.constants import (
    PRODUCTS,
    PAYMENT_METHODS,
    SALES_CHANNELS,
    ORDER_STATUS,
    CUSTOMER_SEGMENTS,
)
from datetime import timedelta
from faker import Faker

from shared.utils import Utils


class SalesGenerator:
    def __init__(self, **kwargs):
        self.faker = Faker()
        self.data_size = kwargs.get("data_size", 100_000_000)
        self.chunk_size = kwargs.get("chunk_size", 500_000)
        self.shared_utils = Utils()
        self.products = PRODUCTS
        self.payment_methods = PAYMENT_METHODS
        self.sales_channels = SALES_CHANNELS
        self.order_status = ORDER_STATUS
        self.customer_segments = CUSTOMER_SEGMENTS

        # Marketing/Channel constants
        self.campaigns = ["Summer Sale", "Black Friday", "Holiday Deals", "Flash Sale"]
        self.traffic_sources = [
            "Google Ads",
            "Facebook Ads",
            "Instagram",
            "Organic",
            "Email",
        ]
        self.devices = ["Desktop", "Mobile", "Tablet"]

    def generate_data(self):
        """Generate sales data in chunks"""
        for chunk in self.shared_utils.chunk_data(
            range(self.data_size),
            self.chunk_size,
        ):
            yield [self.generate_sale() for _ in chunk]

    def generate_sale(self):
        """Generate a single sales transaction"""

        # Generate core transaction info
        order_date = self.faker.date_time_between("-365d", "now")
        order_id = f"ORD-{uuid4().hex[:10].upper()}"

        # Customer info
        customer_id = f"CUST-{random.randint(100000, 999999)}"
        customer_segment = random.choice(self.customer_segments)
        is_returning = random.choice([True, False])

        # Product selection
        product = random.choice(self.products)
        quantity = random.randint(1, 5)

        # Pricing calculation
        base_price = product["base_price"]

        # Apply segment-based discount
        segment_discount = 0
        if customer_segment == "Premium":
            segment_discount = random.uniform(0.10, 0.20)
        elif customer_segment == "Regular":
            segment_discount = random.uniform(0.05, 0.10)

        # Apply campaign discount if applicable
        campaign_discount = 0
        apply_campaign = random.choice([True, False])
        if apply_campaign:
            campaign_discount = random.uniform(0.05, 0.25)

        # Calculate amounts
        unit_price = round(base_price * (1 - segment_discount), 2)
        subtotal = round(unit_price * quantity, 2)
        discount_amount = round(subtotal * campaign_discount, 2)
        tax_amount = round((subtotal - discount_amount) * 0.1, 2)
        shipping_fee = round(random.uniform(0, 15), 2) if subtotal > 0 else 0
        total_amount = round(subtotal - discount_amount + tax_amount + shipping_fee, 2)

        # Profit calculation (assuming 40% margin before discounts)
        cost = round(base_price * 0.6 * quantity, 2)
        profit = round(total_amount - cost - shipping_fee, 2)
        profit_margin = (
            round((profit / total_amount * 100), 2) if total_amount > 0 else 0
        )

        # Sales channel and campaign info
        sales_channel = random.choice(self.sales_channels)
        campaign = random.choice(self.campaigns)
        traffic_source = random.choice(self.traffic_sources)
        device = random.choice(self.devices)

        # Payment info
        payment_method = random.choice(self.payment_methods)
        payment_status = random.choice(["Completed", "Pending", "Failed"])

        # Order status
        status = random.choice(self.order_status)

        # Fulfillment dates
        confirmation_date = order_date + timedelta(minutes=random.randint(1, 30))
        shipped_date = None
        delivered_date = None

        if status in ["Shipped", "Delivered"]:
            shipped_date = (
                order_date + timedelta(days=random.randint(1, 5))
            ).isoformat()

        if status == "Delivered":
            delivered_date = (
                order_date + timedelta(days=random.randint(6, 15))
            ).isoformat()

        return {
            "order_id": order_id,
            "order_date": order_date.isoformat(),
            "confirmation_date": confirmation_date.isoformat(),
            "shipped_date": shipped_date,
            "delivered_date": delivered_date,
            "customer": {
                "customer_id": customer_id,
                "email": self.faker.email(),
                "country": self.faker.country(),
                "city": self.faker.city(),
                "segment": customer_segment,
                "is_returning_customer": is_returning,
            },
            "product": {
                "product_id": product["product_id"],
                "name": product["name"],
                "category": product["category"],
                "base_price": product["base_price"],
                "quantity": quantity,
            },
            "pricing": {
                "unit_price": unit_price,
                "subtotal": subtotal,
                "discount_rate": round(campaign_discount * 100, 2),
                "discount_amount": discount_amount,
                "tax_amount": tax_amount,
                "shipping_fee": shipping_fee,
                "total_amount": total_amount,
                "currency": "USD",
            },
            "profitability": {
                "cost": cost,
                "profit": profit,
                "profit_margin_percent": profit_margin,
            },
            "channel": {
                "sales_channel": sales_channel,
                "campaign": campaign,
                "traffic_source": traffic_source,
                "device": device,
            },
            "payment": {
                "payment_method": payment_method,
                "payment_status": payment_status,
            },
            "order_status": status,
        }
