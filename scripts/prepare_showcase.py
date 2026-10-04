"""
============================================================
 TravelHub — Prepare Showcase Data (~10 Rows)
 Tự động tạo và nạp bộ dữ liệu mẫu showcase (~10 dòng) vào
 landing zone raw/ để chạy pipeline demo.
 
 Hoạt động trơn tru cả bên trong Docker container (/opt)
 lẫn trên máy Host (PowerShell/CMD/Bash).

 Sử dụng:
   python3 scripts/prepare_showcase.py [YYYY-MM-DD]
============================================================
"""

import os
import sys
from datetime import date

SHOWCASE_DATA = {
    ("bookings", "bookings_001.csv"): """booking_id,hotel_id,user_id,room_type,checkin_date,checkout_date,total_amount,currency,booking_status,source_channel,payment_method,created_at
BK-DEMO-001,HTL-001,USR-001,Deluxe,2026-05-10,2026-05-14," 24,500,000 VND ",VND,confirm,app_android,BANK_TRANSFER,2026-05-01 08:30:00
BK-DEMO-002,HTL-001,USR-001,Suite,2026-08-15,2026-08-18,"$25,500,000.00",VND,CONFIRMED,web,CREDIT_CARD,2026-08-01 14:15:00
BK-DEMO-003,HTL-002,USR-002,Standard,2026-09-01,2026-09-05,350.00,USD,PENDING,website,CREDIT_CARD,2026-08-20 09:00:00
BK-DEMO-003,HTL-002,USR-002,Standard,2026-09-01,2026-09-05," 350.00 ",USD,CONFIRMED,website,CREDIT_CARD,2026-08-20 10:15:00
BK-DEMO-004,HTL-002,USR-002,Deluxe,2026-09-10,2026-09-08,"$1,200.00",USD,CONFIRMED,app_ios,CREDIT_CARD,2026-08-25 11:00:00
,HTL-003,USR-003,Standard,2026-09-15,2026-09-18,450.00,USD,CONFIRMED,web,EWALLET,2026-08-28 16:20:00
""",

    ("bookings", "bookings_002.xml"): """<?xml version="1.0" encoding="UTF-8"?>
<bookings>
  <booking>
    <booking_id>BK-DEMO-005</booking_id>
    <hotel_id>HTL-002</hotel_id>
    <user_id>USR-002</user_id>
    <room_type>Deluxe</room_type>
    <checkin_date>2026-09-12</checkin_date>
    <checkout_date>2026-09-15</checkout_date>
    <total_amount>72,000 THB</total_amount>
    <currency>THB</currency>
    <booking_status>1</booking_status>
    <source_channel>partner_ota_agoda</source_channel>
    <payment_method>CREDIT_CARD</payment_method>
    <created_at>2026-08-10 10:20:00</created_at>
  </booking>
  <booking>
    <booking_id>BK-DEMO-006</booking_id>
    <hotel_id>HTL-001</hotel_id>
    <user_id>USR-001</user_id>
    <room_type>Deluxe</room_type>
    <checkin_date>2026-09-20</checkin_date>
    <checkout_date>2026-09-22</checkout_date>
    <total_amount>$500.00</total_amount>
    <currency>USD</currency>
    <booking_status>CANCELLED</booking_status>
    <source_channel>web</source_channel>
    <payment_method>BANK_TRANSFER</payment_method>
    <created_at>2026-08-12 15:40:00</created_at>
  </booking>
  <booking>
    <booking_id>BK-DEMO-007</booking_id>
    <hotel_id>HTL-003</hotel_id>
    <user_id>USR-003</user_id>
    <room_type>Executive</room_type>
    <checkin_date>2026-09-25</checkin_date>
    <checkout_date>2026-09-28</checkout_date>
    <total_amount>1,340 SGD</total_amount>
    <currency>SGD</currency>
    <booking_status>CONFIRMED</booking_status>
    <source_channel>mobile app</source_channel>
    <payment_method>EWALLET</payment_method>
    <created_at>2026-08-15 09:10:00</created_at>
  </booking>
  <booking>
    <booking_id>BK-DEMO-008</booking_id>
    <hotel_id>HTL-003</hotel_id>
    <user_id>USR-003</user_id>
    <room_type>Standard</room_type>
    <checkin_date>2026-09-28</checkin_date>
    <checkout_date>2026-09-30</checkout_date>
    <total_amount>FREE_PROMO_VOUCHER</total_amount>
    <currency>USD</currency>
    <booking_status>CONFIRMED</booking_status>
    <source_channel>web</source_channel>
    <payment_method>CREDIT_CARD</payment_method>
    <created_at>2026-08-18 11:30:00</created_at>
  </booking>
</bookings>
""",

    ("hotels", "hotels_full.xml"): """<?xml version="1.0" encoding="UTF-8"?>
<hotels>
  <hotel>
    <hotel_id>HTL-001</hotel_id>
    <hotel_name>Da Nang Luxury Beach Resort</hotel_name>
    <hotel_tier>luxury</hotel_tier>
    <star_rating>5</star_rating>
    <city>Da Nang</city>
    <country>VN</country>
    <region>Central</region>
    <latitude>16.0544</latitude>
    <longitude>108.2022</longitude>
    <total_rooms>150</total_rooms>
    <facilities>['pool', 'spa', 'wifi', 'beach', 'restaurant']</facilities>
    <partner_since>2022-01-15</partner_since>
    <is_active>true</is_active>
  </hotel>
  <hotel>
    <hotel_id>HTL-002</hotel_id>
    <hotel_name>Bangkok Riverside Hotel</hotel_name>
    <hotel_tier>boutique</hotel_tier>
    <star_rating>4</star_rating>
    <city>Bangkok</city>
    <country>TH</country>
    <region>Central</region>
    <latitude>13.7563</latitude>
    <longitude>100.5018</longitude>
    <total_rooms>80</total_rooms>
    <facilities>['wifi', 'bar', 'pool']</facilities>
    <partner_since>2021-06-10</partner_since>
    <is_active>1</is_active>
  </hotel>
  <hotel>
    <hotel_id>HTL-003</hotel_id>
    <hotel_name>Singapore Grand Central</hotel_name>
    <hotel_tier>luxury</hotel_tier>
    <star_rating>5</star_rating>
    <city>Singapore</city>
    <country>SG</country>
    <region>Central</region>
    <latitude>1.3521</latitude>
    <longitude>103.8198</longitude>
    <total_rooms>220</total_rooms>
    <facilities>['gym', 'wifi', 'conference', 'restaurant']</facilities>
    <partner_since>2020-03-01</partner_since>
    <is_active>yes</is_active>
  </hotel>
  <hotel>
    <hotel_id>HTL-004</hotel_id>
    <hotel_name>Invalid Star Hotel</hotel_name>
    <hotel_tier>budget</hotel_tier>
    <star_rating>7</star_rating>
    <city>Hanoi</city>
    <country>VN</country>
    <region>North</region>
    <latitude>21.0285</latitude>
    <longitude>105.8542</longitude>
    <total_rooms>50</total_rooms>
    <facilities>['wifi']</facilities>
    <partner_since>2024-01-01</partner_since>
    <is_active>true</is_active>
  </hotel>
  <hotel>
    <hotel_id>HTL-005</hotel_id>
    <hotel_name>Negative Room Hotel</hotel_name>
    <hotel_tier>standard</hotel_tier>
    <star_rating>3</star_rating>
    <city>Phuket</city>
    <country>TH</country>
    <region>South</region>
    <latitude>7.8804</latitude>
    <longitude>98.3923</longitude>
    <total_rooms>-15</total_rooms>
    <facilities>['pool']</facilities>
    <partner_since>2023-05-12</partner_since>
    <is_active>false</is_active>
  </hotel>
</hotels>
""",

    ("customers", "crm_export.csv"): """user_id,full_name,email,phone,country,user_segment,loyalty_tier,loyalty_points,first_booking_date,total_bookings,is_active,registered_at
USR-001,Alice Nguyen,alice.nguyen@traveler.vn,+84-901-234-567,VN,premium,Platinum,15200,2023-01-10,12,true,2022-12-01
USR-002,Somchai Prasert,somchai.p@thai-traveler.th,+66 81 234 5678,TH,standard,Gold,8400,2024-02-15,6,1,2023-11-20
USR-003,David Tan,david.tan@sgmail.sg,+65 9123 4567,SG,vip,Diamond,32000,2022-05-20,25,yes,2021-08-14
USR-004,Bob Blank Email,,+84912345678,VN,budget,Bronze,500,2026-01-05,1,true,2025-12-28
USR-005,Charlie No Phone,charlie.np@sample.org,,TH,standard,Silver,2100,2025-08-10,3,true,2025-06-15
""",

    ("payments", "payments.csv"): """payment_id,booking_id,amount,currency,payment_method,payment_status,gateway_code,processed_at,refund_amount,refund_at
PAY-001,BK-DEMO-001," 24,500,000 VND ",VND,BANK_TRANSFER,SUCCESS,VNPAY_991,2026-05-01 08:35:00,0,
PAY-002,BK-DEMO-002,"$25,500,000.00",VND,CREDIT_CARD,SUCCESS,STRIPE_002,2026-08-01 14:20:00,0,
PAY-003,BK-DEMO-003,350.00,USD,CREDIT_CARD,SUCCESS,STRIPE_003,2026-08-20 10:20:00,0,
PAY-004,BK-DEMO-005," 72,000 THB ",THB,CREDIT_CARD,SUCCESS,2C2P_TH_01,2026-08-10 10:25:00,0,
PAY-005,BK-DEMO-006,$500.00,USD,BANK_TRANSFER,SUCCESS,STRIPE_REF,2026-08-12 15:45:00,500.00,2026-08-13 10:00:00
PAY-006,BK-DEMO-007,"INVALID_MONEY",SGD,EWALLET,FAILED,GRABPAY,2026-08-15 09:15:00,0,
""",

    ("clickstream", "events_001.csv"): """event_id,session_id,user_id,event_type,page_url,hotel_id,search_query,device_type,os,event_timestamp
EVT-001,SES-001,USR-001,page_view,https://travelhub.com/hotels/HTL-001,HTL-001,,mobile,Android,2026-05-01 08:15:00
EVT-002,SES-001,USR-001,booking_submit,https://travelhub.com/checkout,HTL-001,,mobile,Android,2026-05-01 08:30:00
EVT-003,SES-002,USR-002,search,https://travelhub.com/search,HTL-002,Bangkok luxury hotel,desktop,Windows,2026-08-20 08:50:00
EVT-004,SES-002,USR-002,booking_submit,https://travelhub.com/checkout,HTL-002,,desktop,Windows,2026-08-20 09:00:00
EVT-005,SES-003,USR-003,page_view,https://travelhub.com/hotels/HTL-003,HTL-003,,tablet,iOS,2026-08-15 09:05:00
"""
}

def get_raw_root():
    # 1. Nếu đang chạy trong container (/opt/sample-data)
    if os.path.isdir("/opt/sample-data"):
        return "/opt/sample-data/raw"
    
    # 2. Nếu đang chạy ở máy Host (Windows / Linux)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "data", "sample", "raw")

def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else str(date.today())
    raw_root = get_raw_root()

    print(f"📦 Đang chuẩn bị dữ liệu Showcase (~10 dòng) cho ngày: {target_date}...")
    print(f"   Thư mục đích: {raw_root}")

    for (entity, filename), content in SHOWCASE_DATA.items():
        dest_dir = os.path.join(raw_root, entity, f"dt={target_date}")
        os.makedirs(dest_dir, exist_ok=True)
        dest_path = os.path.join(dest_dir, filename)
        
        with open(dest_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  ✅ [{entity}] ➔ {dest_path}")

    print("\n🎉 Dữ liệu đã sẵn sàng!")
    print(f"🚀 Để chạy pipeline demo, hãy chạy lệnh sau:")
    print(f"   python3 /opt/scripts/run_pipeline.py {target_date} --skip-generate\n")

if __name__ == "__main__":
    main()
