import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from google.cloud import pubsub_v1
from config import project_id, payment_result_sub_id


subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, payment_result_sub_id)


def send_payment_notification(payment_result):
    order_id = payment_result["order_id"]
    customer = payment_result["customer"]
    product = payment_result["product"]
    total_amount = payment_result["total_amount"]
    payment_status = payment_result["payment_status"]
    reason = payment_result["reason"]

    print("\n[Notification Service] Xác nhận thanh toán đơn hàng")
    print("[Notification Service] Order ID:", order_id)
    print("[Notification Service] Customer:", customer)
    print("[Notification Service] Product:", product)
    print("[Notification Service] Total amount:", total_amount, "VND")
    print("[Notification Service] Payment status:", payment_status)

    print("[Notification Service] Đang gửi thông báo đến khách hàng...")
    time.sleep(3)

    if payment_status == "Thanh toán thành công":
        print("[Notification Service] Thông báo:")
        print(
            "Kính gửi khách hàng",
            customer + ",",
            "đơn hàng",
            order_id,
            "của bạn đã thanh toán thành công."
        )

    elif payment_status == "Thanh toán thất bại":
        print("[Notification Service] Lý do:", reason)
        print("[Notification Service] Thông báo:")
        print(
            "Kính gửi khách hàng",
            customer + ",",
            "đơn hàng",
            order_id,
            " của bạn thanh toán không thành công. Vui lòng chọn phương thức thanh toán khác."
        )

    else:
        print("[Notification Service] Trạng thái thanh toán không xác định:", payment_status)

    print("[Notification Service] Status: Xác nhận thanh toán đơn hàng")


def callback(message):
    try:
        data = message.data.decode("utf-8")
        payment_result = json.loads(data)

        send_payment_notification(payment_result)

        message.ack()
        print("[Notification Service] Đã xác nhận tin nhắn")

    except Exception as e:
        print("[Notification Service] Error:", e)
        message.nack()


print("[Notification Service] Listening for payment result messages...")
print("[Notification Service] Subscription:", subscription_path)

streaming_pull_future = subscriber.subscribe(
    subscription_path,
    callback=callback
)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\n[Notification Service] Stopped")