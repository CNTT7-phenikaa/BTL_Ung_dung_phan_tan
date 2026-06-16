import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from google.cloud import pubsub_v1
from config import project_id, payment_result_sub_id


subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, payment_result_sub_id)
flow_control = pubsub_v1.types.FlowControl(max_messages=1)

def format_items(items):
    item_texts = []

    for item in items:
        item_text = (
            item["product"]
            + " x"
            + str(item["quantity"])
            + " - "
            + str(item["price"])
            + " VND"
        )
        item_texts.append(item_text)

    return ", ".join(item_texts)

def send_payment_notification(payment_result):
    print("\n" + "=" * 60)
    order_id = payment_result["order_id"]
    customer = payment_result["customer"]
    items = payment_result["items"]
    total_amount = payment_result["total_amount"]
    inventory_status = payment_result["inventory_status"]
    payment_status = payment_result["payment_status"]
    reason = payment_result["reason"]
    sp_thieu = payment_result.get("sp_thieu", [])
    
    print("\n" + "=" * 60)
    print("[Notification Service] Xác nhận thanh toán đơn hàng")
    print("[Notification Service] Order ID:", order_id)
    print("[Notification Service] Customer:", customer)
    print("[Notification Service] Items:", format_items(items))
    print("[Notification Service] Total amount:", total_amount, "VND")
    print("[Notification Service] Inventory status:", inventory_status)
    print("[Notification Service] Payment status:", payment_status)

    print("[Notification Service] Đang gửi thông báo đến khách hàng...")
    time.sleep(1)

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
    
    elif payment_status == "Không thể thực hiện thanh toán":
        print("[Notification Service] Reason:", reason)

        if len(sp_thieu) > 0:
            print("[Notification Service] Không đủ hàng trong kho:")
            for item in sp_thieu:
                print(
                    "[Notification Service]",
                    item["product"],
                    "| Yêu cầu:",
                    item["requested_quantity"],
                    "| Tồn kho:",
                    item["available_quantity"]
                )

        print("[Notification Service] Message:")
        print(
            "Kính gửi khách hàng",
            customer + ",",
            "đơn hàng",
            order_id,
            "không thể thanh toán vì một số sản phẩm đã hết hàng. Xin lỗi vì sự bất tiện này"
        )

    else:
        print("[Notification Service] Trạng thái thanh toán không xác định:", payment_status)

    print("[Notification Service] Status: Xác nhận trạng thái thanh toán đơn hàng")


def callback(message):
    try:
        data = message.data.decode("utf-8")
        payment_result = json.loads(data)

        send_payment_notification(payment_result)

        message.ack()
        print("[Notification Service] Đã xác nhận yêu cầu gửi tin nhắn")

    except Exception as e:
        print("[Notification Service] Error:", e)
        if "items" not in payment_result:
            message.ack()
            return
          


print("[Notification Service] Chờ xác nhận thanh toán mới...")
print("[Notification Service] Subscription:", subscription_path)

streaming_pull_future = subscriber.subscribe(
    subscription_path,
    callback=callback,
    flow_control=flow_control
)


try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\n[Notification Service] Stopped")