import uuid
import random
from datetime import datetime, timedelta
import pandas as pd
import os
import json

n_users = 100
sessions_per_user = 5

products = [
    {"product_id": "sku_1", "price": 35.50, "category": "apparel"},
    {"product_id": "sku_2", "price": 60.42, "category": "home"},
    {"product_id": "sku_3", "price": 103.64, "category": "electronic"},
]

script_dir = os.path.dirname(os.path.abspath(__file__))
folder_path = os.path.join(script_dir, "data")
os.makedirs(folder_path, exist_ok=True)

for day in range(1, 29):
    data = []
    base_time = datetime(2026, 2, day)

    for user in range(1, n_users + 1):
        user_base_time = base_time
        for session_num in range(sessions_per_user):
            session_id = str(uuid.uuid4())
            session_start = user_base_time + timedelta(minutes=session_num * 60 + random.randint(0, 59))
            session_time = session_start

            # 1️⃣ product view
            for _ in range(random.randint(1,3)):
                product = random.choice(products)
                data.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": f"user_{user}",
                    "session_id": session_id,
                    "event_type": "product_view",
                    "page_url": f"/product/{product['product_id']}",
                    "product_id": product["product_id"],
                    "price": product["price"],
                    "event_timestamp": session_time.isoformat()
                })

                session_time += timedelta(seconds=random.randint(10,20))

            # 2️⃣ maybe add to cart
            if random.random() > 0.3:
                for _ in range(random.randint(1,2)):
                    product = random.choice(products)
                    data.append({
                        "event_id": str(uuid.uuid4()),
                        "user_id": f"user_{user}",
                        "session_id": session_id,
                        "event_type": "add_to_cart",
                        "page_url": "/cart",
                        "product_id": product["product_id"],
                        "price": product["price"],
                        "quantity": random.randint(1, 3),
                        "event_timestamp": session_time.isoformat()
                    })

                    session_time += timedelta(seconds=random.randint(120,150))

                # 3️⃣ maybe purchase
                if random.random() > 0.5:
                    product = random.choice(products)
                    data.append({
                        "event_id": str(uuid.uuid4()),
                        "user_id": f"user_{user}",
                        "session_id": session_id,
                        "event_type": "purchase",
                        "page_url": "/checkout",
                        "product_id": product["product_id"],
                        "price": product["price"],
                        "quantity": random.randint(1, 3),
                        "event_timestamp": session_time.isoformat()
                    })

                    session_time += timedelta(seconds=random.randint(120,150))

    df = pd.DataFrame(data)
    file_name = f"events_02-{day}-26.json"
    records = df.to_dict(orient="records")
    with open(f"{folder_path}/{file_name}", "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")