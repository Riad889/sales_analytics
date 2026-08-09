import random

from uuid import uuid4
from generators.constants import HOTELS, CAMPAIGNS
from datetime import timedelta
from faker import Faker

from .constants import LOYALTY, TRAFFIC_SOURCES, DEVICES
from shared.utils import Utils


class BookingGenerator:
    def __init__(self, **kwargs):
        self.faker = Faker()
        self.data_size = kwargs.get("data_size", 100_00_000)
        self.chunk_size = kwargs.get("chunk_size", 100_000)
        self.shared_utils = Utils()
        self.hotels = HOTELS
        self.campaigns = CAMPAIGNS

    def generate_data(self):
        for chunk in self.shared_utils.chunk_data(
            range(self.data_size),
            self.chunk_size,
        ):
            yield [self.generate_booking() for _ in chunk]

    def generate_booking(self):

        hotel = random.choice(self.hotels)

        booking_date = self.faker.date_time_between("-365d", "now")

        nights = random.randint(1, 10)

        check_in = booking_date + timedelta(days=random.randint(1, 120))

        check_out = check_in + timedelta(days=nights)

        rooms = random.randint(1, 3)

        adults = random.randint(1, rooms * 2)

        children = random.randint(0, 2)

        booking_value = round(random.uniform(80, 2500), 2)

        discount = round(booking_value * random.uniform(0, 0.15), 2)

        tax = round(booking_value * 0.08, 2)

        service_fee = round(booking_value * 0.02, 2)

        net_sale = booking_value - discount

        commission_rate = random.choice([3, 4, 5])

        commission = round(net_sale * commission_rate / 100, 2)

        return {
            "booking_id": f"BK-{uuid4().hex[:10].upper()}",
            "affiliate_booking_reference": f"AFF-{uuid4().hex[:8].upper()}",
            "status": random.choice(["confirmed", "cancelled", "pending"]),
            "booking_date": booking_date.isoformat(),
            "confirmation_date": (booking_date + timedelta(minutes=2)).isoformat(),
            "travel": {
                "check_in": check_in.isoformat(),
                "check_out": check_out.isoformat(),
                "nights": nights,
                "rooms": rooms,
                "adults": adults,
                "children": children,
                "total_guests": adults + children,
            },
            "guest": {
                "country": self.faker.country(),
                "city": self.faker.city(),
                "is_returning_customer": random.choice([True, False]),
                "loyalty_level": random.choice(LOYALTY),
            },
            "property": {
                **hotel,
                "review_score": round(random.uniform(7, 9.8), 1),
                "review_count": random.randint(100, 20000),
            },
            "sales": {
                "booking_value": booking_value,
                "currency": "USD",
                "discount": discount,
                "tax_amount": tax,
                "service_fee": service_fee,
                "net_sale": net_sale,
                "commission_rate": commission_rate,
                "estimated_commission": commission,
            },
            "affiliate": {
                "affiliate_id": f"AFF-{random.randint(1000, 9999)}",
                "campaign": random.choice(CAMPAIGNS),
                "traffic_source": random.choice(TRAFFIC_SOURCES),
                "device": random.choice(DEVICES),
            },
        }
