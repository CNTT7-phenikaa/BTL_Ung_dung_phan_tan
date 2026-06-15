import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from google.cloud import pubsub_v1
from BTL_Ung_dung_phan_tan.config import project_id, order_topic_id

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, order_topic_id)

orders = [
    {
        "order_id": "ORD001",
        "customer": "Nguyen Quynh Trang",
        "product": "Laptop",
        "quantity": 1,
        "price": 15000000
    },
    {
        "order_id": "ORD002",
        "customer": "Nguyen Van A",
        "product": "Mouse",
        "quantity": 2,
       "price": 300000
    },
    {
        "order_id": "ORD003",
        "customer": "Nguyen Thi B",
        "product": "Keyboard",
        "quantity": 1,
        "price": 500000
    }
]

for order in orders:
    data = json.dumps(order).encode("utf-8")

    future = publisher.publish(topic_path, data)

    print("[Publisher] Sent order:", order["order_id"])
    print("[Publisher] Message ID:", future.result())

    time.sleep(2)