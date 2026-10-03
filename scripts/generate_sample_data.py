"""
============================================================
 TravelHub — Sample Data Generator
 Tạo dữ liệu mẫu CSV/XML cho 5 nguồn dữ liệu
 Sử dụng: python generate_sample_data.py [date]
============================================================
"""

import os
import csv
import random
import hashlib
import uuid
import sys
from datetime import datetime, timedelta, date
from xml.etree.ElementTree import Element, SubElement, tostring
try:
    from xml.etree.ElementTree import indent
except ImportError:
    def indent(elem, level=0):
        i = "\n" + level * "  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for child in elem:
                indent(child, level + 1)
            if not child.tail or not child.tail.strip():
                child.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i


# ──────────────── CONFIG ────────────────
NUM_HOTELS = 50
NUM_CUSTOMERS = 200
NUM_BOOKINGS = 500
NUM_PAYMENTS = 450
NUM_CLICKSTREAM = 2000

COUNTRIES = ["VN", "TH", "SG", "ID"]
CITIES = {
    "VN": ["Ho Chi Minh City", "Hanoi", "Da Nang", "Nha Trang", "Phu Quoc", "Hue"],
    "TH": ["Bangkok", "Chiang Mai", "Phuket", "Pattaya"],
    "SG": ["Singapore"],
    "ID": ["Jakarta", "Bali", "Yogyakarta", "Bandung"],
}
REGIONS = {
    "Ho Chi Minh City": "South", "Hanoi": "North", "Da Nang": "Central",
    "Nha Trang": "Central", "Phu Quoc": "South", "Hue": "Central",
    "Bangkok": "Central", "Chiang Mai": "North", "Phuket": "South", "Pattaya": "Central",
    "Singapore": "Central",
    "Jakarta": "Java", "Bali": "Bali", "Yogyakarta": "Java", "Bandung": "Java",
}
ROOM_TYPES = ["Standard", "Deluxe", "Suite", "Family", "Executive"]
HOTEL_TIERS = ["budget", "standard", "luxury"]
BOOKING_STATUSES = ["CONFIRMED", "CANCELLED", "PENDING", "NO_SHOW",
                    "confirm", "CONFIRM", "1"]  # intentional dirty data
CHANNELS = ["web", "website", "app_ios", "app_android", "partner_abc", "partner_xyz"]
PAYMENT_METHODS = ["CREDIT_CARD", "BANK_TRANSFER", "EWALLET"]
PAYMENT_STATUSES = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]
SEGMENTS = ["budget", "standard", "premium"]
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
DEVICES = ["desktop", "mobile", "tablet"]
OS_LIST = ["iOS", "Android", "Windows", "macOS"]
EVENT_TYPES = ["page_view", "search", "click", "booking_start", "booking_complete"]
FACILITIES = ["pool", "gym", "spa", "restaurant", "bar", "wifi", "parking",
              "conference", "beach", "garden"]

FIRST_NAMES = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Dang", "Bui",
               "Do", "Ngo", "Somchai", "Siti", "Ahmad", "Dewi", "Putri",
               "Tan", "Lim", "Wong", "Chan", "Kumar"]
LAST_NAMES = ["Anh", "Minh", "Huy", "Linh", "Thanh", "Duc", "Tuan", "Mai",
              "Hoa", "Phuong", "Wattana", "Sari", "Rahman", "Lee", "Ng"]


def gen_date_str():
    return execution_date


def gen_id(prefix, n):
    return f"{prefix}-{str(n).zfill(8)}"


def random_datetime(base_date_str):
    base = datetime.strptime(base_date_str, "%Y-%m-%d")
    return base + timedelta(hours=random.randint(0, 23),
                            minutes=random.randint(0, 59),
                            seconds=random.randint(0, 59))


def amount_dirty(value):
    """Produce intentionally messy amount strings."""
    formats = [
        f"{value:,.0f} VND",
        f"{value:,.2f}",
        f"${value:,.2f}",
        f"{value}",
        f" {value:,.0f} ",
    ]
    return random.choice(formats)


# ──────────────── GENERATORS ────────────────

def generate_hotels(output_dir):
    """Generate hotels_full.xml — full daily snapshot."""
    root = Element("hotels")
    hotels = []

    for i in range(1, NUM_HOTELS + 1):
        hotel_id = gen_id("HTL", i)
        country = random.choice(COUNTRIES)
        city = random.choice(CITIES[country])
        region = REGIONS[city]
        tier = random.choice(HOTEL_TIERS)
        star = random.randint(1, 5)
        rooms = random.randint(20, 500)
        facs = random.sample(FACILITIES, random.randint(2, 6))

        hotel = Element("hotel")
        SubElement(hotel, "hotel_id").text = hotel_id
        SubElement(hotel, "hotel_name").text = f"{city} {tier.title()} Hotel {i}"
        SubElement(hotel, "hotel_tier").text = tier
        SubElement(hotel, "star_rating").text = str(star)
        SubElement(hotel, "city").text = city
        SubElement(hotel, "country").text = country
        SubElement(hotel, "region").text = region
        SubElement(hotel, "latitude").text = f"{random.uniform(1.0, 21.0):.6f}"
        SubElement(hotel, "longitude").text = f"{random.uniform(100.0, 115.0):.6f}"
        SubElement(hotel, "total_rooms").text = str(rooms)
        SubElement(hotel, "facilities").text = str(facs)
        SubElement(hotel, "partner_since").text = f"20{random.randint(18, 25)}-{random.randint(1,12):02d}-01"
        SubElement(hotel, "is_active").text = random.choice(["true", "true", "true", "false"])
        root.append(hotel)

        hotels.append({
            "hotel_id": hotel_id, "city": city, "country": country,
            "region": region, "tier": tier, "star_rating": star,
            "total_rooms": rooms
        })

    indent(root)
    xml_path = os.path.join(output_dir, "hotels", f"dt={execution_date}")
    os.makedirs(xml_path, exist_ok=True)
    with open(os.path.join(xml_path, "hotels_full.xml"), "wb") as f:
        f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write(tostring(root, encoding="unicode").encode("utf-8"))

    print(f"  ✅ Generated {NUM_HOTELS} hotels → {xml_path}")
    return hotels


def generate_customers(output_dir):
    """Generate crm_export.csv."""
    customers = []
    csv_path = os.path.join(output_dir, "customers", f"dt={execution_date}")
    os.makedirs(csv_path, exist_ok=True)

    with open(os.path.join(csv_path, "crm_export.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "full_name", "email", "phone", "country",
                         "user_segment", "loyalty_tier", "loyalty_points",
                         "first_booking_date", "total_bookings", "is_active",
                         "registered_at"])

        for i in range(1, NUM_CUSTOMERS + 1):
            user_id = gen_id("USR", i)
            fname = random.choice(FIRST_NAMES)
            lname = random.choice(LAST_NAMES)
            full_name = f"{fname} {lname}"
            email = f"{fname.lower()}.{lname.lower()}{i}@email.com"
            phone = f"+84{random.randint(100000000, 999999999)}"
            country = random.choice(COUNTRIES)
            segment = random.choice(SEGMENTS)
            tier = random.choice(LOYALTY_TIERS)
            points = random.randint(0, 50000)
            first_booking = f"20{random.randint(20, 26)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            total_bk = random.randint(1, 50)
            is_active = random.choice(["true", "true", "true", "false"])
            reg_date = f"20{random.randint(18, 25)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"

            writer.writerow([user_id, full_name, email, phone, country,
                             segment, tier, points, first_booking, total_bk,
                             is_active, reg_date])
            customers.append({"user_id": user_id, "country": country})

    print(f"  ✅ Generated {NUM_CUSTOMERS} customers → {csv_path}")
    return customers


def generate_bookings(output_dir, hotels, customers):
    """Generate bookings CSV + XML (mixed format intentionally)."""
    csv_path = os.path.join(output_dir, "bookings", f"dt={execution_date}")
    os.makedirs(csv_path, exist_ok=True)

    bookings = []
    csv_rows = []
    xml_bookings = []

    for i in range(1, NUM_BOOKINGS + 1):
        booking_id = gen_id("BK", i)
        hotel = random.choice(hotels)
        customer = random.choice(customers)
        room_type = random.choice(ROOM_TYPES)
        base_date = datetime.strptime(execution_date, "%Y-%m-%d")
        checkin = base_date + timedelta(days=random.randint(1, 90))
        checkout = checkin + timedelta(days=random.randint(1, 14))
        amount = random.randint(500000, 50000000)
        currency = random.choice(["VND", "USD", "THB", "SGD", "IDR"])
        status = random.choice(BOOKING_STATUSES)
        channel = random.choice(CHANNELS)
        payment = random.choice(PAYMENT_METHODS)
        created_at = random_datetime(execution_date).strftime("%Y-%m-%d %H:%M:%S")

        record = {
            "booking_id": booking_id,
            "hotel_id": hotel["hotel_id"],
            "user_id": customer["user_id"],
            "room_type": room_type,
            "checkin_date": checkin.strftime("%Y-%m-%d"),
            "checkout_date": checkout.strftime("%Y-%m-%d"),
            "total_amount": amount_dirty(amount),  # intentionally dirty
            "currency": currency,
            "booking_status": status,
            "source_channel": channel,
            "payment_method": payment,
            "created_at": created_at,
        }

        bookings.append({
            "booking_id": booking_id,
            "hotel_id": hotel["hotel_id"],
            "user_id": customer["user_id"],
            "amount_clean": amount,
            "created_at": created_at,
        })

        # Split into CSV and XML (70/30)
        if random.random() < 0.7:
            csv_rows.append(record)
        else:
            xml_bookings.append(record)

    # Add some duplicates (intentional dirty data)
    for _ in range(10):
        csv_rows.append(random.choice(csv_rows))

    # Write CSV
    with open(os.path.join(csv_path, "bookings_001.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys()))
        writer.writeheader()
        writer.writerows(csv_rows)

    # Write XML
    root = Element("bookings")
    for rec in xml_bookings:
        booking_el = Element("booking")
        for k, v in rec.items():
            SubElement(booking_el, k).text = str(v)
        root.append(booking_el)
    indent(root)
    with open(os.path.join(csv_path, "bookings_002.xml"), "wb") as f:
        f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write(tostring(root, encoding="unicode").encode("utf-8"))

    print(f"  ✅ Generated {NUM_BOOKINGS} bookings ({len(csv_rows)} CSV + {len(xml_bookings)} XML) → {csv_path}")
    return bookings


def generate_payments(output_dir, bookings):
    """Generate payments.csv."""
    csv_path = os.path.join(output_dir, "payments", f"dt={execution_date}")
    os.makedirs(csv_path, exist_ok=True)

    with open(os.path.join(csv_path, "payments.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["payment_id", "booking_id", "amount", "currency",
                         "payment_method", "payment_status", "gateway_code",
                         "processed_at", "refund_amount", "refund_at"])

        used_bookings = random.sample(bookings, min(NUM_PAYMENTS, len(bookings)))
        for i, bk in enumerate(used_bookings, 1):
            payment_id = gen_id("PAY", i)
            status = random.choice(PAYMENT_STATUSES)
            refund = bk["amount_clean"] * 0.5 if status == "REFUNDED" else 0
            processed = random_datetime(execution_date).strftime("%Y-%m-%d %H:%M:%S")
            refund_at = random_datetime(execution_date).strftime("%Y-%m-%d %H:%M:%S") if refund > 0 else ""
            gateway = random.choice(["00", "01", "02", "E01", "E02", "TIMEOUT"])

            writer.writerow([
                payment_id, bk["booking_id"], bk["amount_clean"],
                random.choice(["VND", "USD", "THB"]),
                random.choice(PAYMENT_METHODS), status, gateway,
                processed, refund, refund_at
            ])

    print(f"  ✅ Generated {NUM_PAYMENTS} payments → {csv_path}")


def generate_clickstream(output_dir, hotels, customers):
    """Generate web_events.csv."""
    csv_path = os.path.join(output_dir, "clickstream", f"dt={execution_date}")
    os.makedirs(csv_path, exist_ok=True)

    sessions = [str(uuid.uuid4())[:8] for _ in range(NUM_CLICKSTREAM // 5)]

    with open(os.path.join(csv_path, "web_events.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["event_id", "session_id", "user_id", "event_type",
                         "page_url", "hotel_id", "search_query", "device_type",
                         "os", "event_timestamp"])

        for i in range(1, NUM_CLICKSTREAM + 1):
            event_id = gen_id("EVT", i)
            session = random.choice(sessions)
            user = random.choice(customers)["user_id"] if random.random() > 0.3 else ""
            event_type = random.choice(EVENT_TYPES)
            hotel = random.choice(hotels)["hotel_id"] if event_type in ["click", "booking_start", "booking_complete"] else ""
            search = random.choice(["beach resort", "luxury hotel", "budget stay",
                                    "family room", ""]) if event_type == "search" else ""
            page_url = random.choice([
                "/", "/search", f"/hotel/{hotel}", "/booking/confirm",
                "/account", "/promotions",
            ])
            device = random.choice(DEVICES)
            os_name = random.choice(OS_LIST)
            ts = random_datetime(execution_date).strftime("%Y-%m-%d %H:%M:%S")

            writer.writerow([event_id, session, user, event_type, page_url,
                             hotel, search, device, os_name, ts])

    print(f"  ✅ Generated {NUM_CLICKSTREAM} clickstream events → {csv_path}")


# ──────────────── MAIN ────────────────

if __name__ == "__main__":
    execution_date = sys.argv[1] if len(sys.argv) > 1 else date.today().strftime("%Y-%m-%d")
    
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]
    elif "RAW_PATH" in os.environ:
        output_dir = os.environ["RAW_PATH"]
    elif os.path.isdir("/opt/sample-data/raw"):
        output_dir = "/opt/sample-data/raw"
    else:
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "sample", "raw")

    print(f"\n🔧 Generating sample data for date: {execution_date}")
    print(f"   Output: {output_dir}\n")

    os.makedirs(output_dir, exist_ok=True)

    hotels = generate_hotels(output_dir)
    customers = generate_customers(output_dir)
    bookings = generate_bookings(output_dir, hotels, customers)
    generate_payments(output_dir, bookings)
    generate_clickstream(output_dir, hotels, customers)

    print(f"\n🎉 Sample data generation complete for {execution_date}!\n")
