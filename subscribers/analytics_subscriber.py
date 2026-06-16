import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from google.cloud import pubsub_v1
from config import project_id, analytics_sub_id


subscriber = pubsub_v1.SubscriberClient()
flow_control = pubsub_v1.types.FlowControl(max_messages=1)

analytics_subscription_path = subscriber.subscription_path(
    project_id,
    analytics_sub_id
)


def calculate_total_amount(items):
    total_amount = 0

    for item in items:
        total_amount += item["quantity"] * item["price"]

    return total_amount


def classify_order(total_amount):
    if total_amount < 1000000:
        return "Đơn hàng giá trị thấp"
    elif total_amount < 10000000:
        return "Đơn hàng giá trị trung bình"
    else:
        return "Đơn hàng giá trị cao"


def analyze_order(order):
    order_id = order["order_id"]
    customer = order["customer"]
    items = order["items"]

    total_amount = calculate_total_amount(items)
    order_type = classify_order(total_amount)

    print("\n" + "=" * 60)
    print("[Analytics Service] Tiếp nhận đơn hàng mới")
    print("[Analytics Service] Order ID:", order_id)
    print("[Analytics Service] Customer:", customer)
    print("[Analytics Service] Danh sách sản phẩm:")

    for item in items:
        item_total = item["quantity"] * item["price"]

        print(
            "[Analytics Service]",
            item["product"],
            "| Số lượng:",
            item["quantity"],
            "| Đơn giá:",
            item["price"],
            "VND",
            "| Thành tiền:",
            item_total,
            "VND"
        )

    print("[Analytics Service] Tổng giá trị đơn hàng:", total_amount, "VND")
    print("[Analytics Service] Phân loại:", order_type)

    time.sleep(1)

    print("[Analytics Service] Đã ghi nhận dữ liệu phân tích đơn hàng")


def callback(message):
    try:
        data = message.data.decode("utf-8")
        order = json.loads(data)

        analyze_order(order)

        message.ack()
        print("[Analytics Service] Đã xác nhận tin nhắn")

    except Exception as e:
        print("[Analytics Service] Error:", e)
        if "items" not in order:
            message.ack()
            return


print("[Analytics Service] Đang chờ đơn hàng mới...")
print("[Analytics Service] Subscription:", analytics_subscription_path)


streaming_pull_future = subscriber.subscribe(
    analytics_subscription_path,
    callback=callback,
    flow_control=flow_control
)


try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\n[Analytics Service] Stopped")