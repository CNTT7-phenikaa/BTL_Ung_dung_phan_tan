import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from google.cloud import pubsub_v1
from config import project_id, email_sub_id


subscriber = pubsub_v1.SubscriberClient()
flow_control = pubsub_v1.types.FlowControl(max_messages=1)

email_subscription_path = subscriber.subscription_path(
    project_id,
    email_sub_id
)


def format_items(items):
    item_texts = []

    for item in items:
        item_total = item["quantity"] * item["price"]

        item_text = (
            item["product"]
            + " x"
            + str(item["quantity"])
            + " - "
            + str(item_total)
            + " VND"
        )

        item_texts.append(item_text)

    return ", ".join(item_texts)


def calculate_total_amount(items):
    total_amount = 0

    for item in items:
        total_amount += item["quantity"] * item["price"]

    return total_amount


def send_order_received_email(order):
    order_id = order["order_id"]
    customer = order["customer"]
    items = order["items"]

    total_amount = calculate_total_amount(items)

    print("\n" + "=" * 60)
    print("[Email Service] Tiếp nhận đơn hàng mới")
    print("[Email Service] Order ID:", order_id)
    print("[Email Service] Customer:", customer)
    print("[Email Service] Items:", format_items(items))
    print("[Email Service] Total amount:", total_amount, "VND")

    print("[Email Service] Đang gửi email tiếp nhận đơn hàng...")
    time.sleep(1)

    print("[Email Service] Nội dung email:")
    print(
        "Kính gửi khách hàng",
        customer + ",",
        "hệ thống đã tiếp nhận đơn hàng",
        order_id,
        "của bạn."
    )
    print(
        "Đơn hàng đang được kiểm tra tồn kho và xử lý thanh toán.",
        "Chúng tôi sẽ thông báo kết quả cuối cùng sau."
    )

    print("[Email Service] Trạng thái: Đã gửi email tiếp nhận đơn hàng")


def callback(message):
    try:
        data = message.data.decode("utf-8")
        order = json.loads(data)

        send_order_received_email(order)

        message.ack()
        print("[Email Service] Đã xác nhận tin nhắn")

    except Exception as e:
        print("[Email Service] Error:", e)
        message.nack()


print("[Email Service] Đang chờ đơn hàng mới...")
print("[Email Service] Subscription:", email_subscription_path)


streaming_pull_future = subscriber.subscribe(
    email_subscription_path,
    callback=callback,
    flow_control=flow_control
)


try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\n[Email Service] Stopped")