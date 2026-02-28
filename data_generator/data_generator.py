import uuid
import random
from datetime import datetime, timedelta
import pandas as pd
import os
import json

n_users = 100
sessions_per_user = 3

products = [
    {"product_id": "sku_1", "price": 29.99, "category": "apparel"},
    {"product_id": "sku_2", "price": 59.99, "category": "home"},
    {"product_id": "sku_3", "price": 99.99, "category": "electronic"},
]

data = []
base_time = datetime.now()

for user in range(1, n_users + 1):
    for _ in range(sessions_per_user):

        session_id = str(uuid.uuid4())
        session_time = base_time + timedelta(minutes=random.randint(0, 5000))

        # 1️⃣ page view
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

        # 2️⃣ maybe add to cart
        if random.random() > 0.3:
            session_time += timedelta(seconds=10)
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

            # 3️⃣ maybe purchase
            if random.random() > 0.5:
                session_time += timedelta(seconds=15)
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

df = pd.DataFrame(data)
for n in range(1, 29):
    file_name = f"events_02-{n}-26.json"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(script_dir, "data")
    os.makedirs(folder_path, exist_ok=True)

    records = df.to_dict(orient="records")

    with open(f"{folder_path}/{file_name}", "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    # df.to_json(f"{folder_path}/{file_name}", orient="records", indent=9)