import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import threading
from datetime import datetime
from google.cloud import pubsub_v1
from config import (
    project_id,
    tracking_inventory_result_sub_id,
    tracking_payment_result_sub_id
)


subscriber = pubsub_v1.SubscriberClient()
flow_control = pubsub_v1.types.FlowControl(max_messages=1)

inventory_tracking_subscription_path = subscriber.subscription_path(
    project_id,
    tracking_inventory_result_sub_id
)

payment_tracking_subscription_path = subscriber.subscription_path(
    project_id,
    tracking_payment_result_sub_id
)


base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
thu_muc = os.path.join(base_dir, "logs")
file_log = os.path.join(thu_muc, "order_tracking.log")

os.makedirs(thu_muc, exist_ok=True)

order_status_history = {}
lock = threading.Lock()


def write_log(order_id, service_name, status, reason):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_line = (
        timestamp
        + " | "
        + order_id
        + " | "
        + service_name
        + " | "
        + status
        + " | "
        + reason
        + "\n"
    )

    with open(file_log, "a", encoding="utf-8") as file:
        file.write(log_line)


def update_order_history(order_id, service_name, status, reason):
    if order_id not in order_status_history:
        order_status_history[order_id] = []

    order_status_history[order_id].append({
        "service": service_name,
        "status": status,
        "reason": reason
    })


def print_order_history(order_id):
    print("\n[Order Tracking Service] Lịch sử xử lý hiện tại của đơn hàng:", order_id)

    for step in order_status_history[order_id]:
        print(
            "[Order Tracking Service]",
            step["service"],
            "->",
            step["status"],
            "|",
            step["reason"]
        )


def handle_inventory_result(inventory_result):
    order_id = inventory_result["order_id"]
    inventory_status = inventory_result["inventory_status"]
    reason = inventory_result["reason"]

    with lock:
        print("\n" + "=" * 60)
        print("[Order Tracking Service] Nhận kết quả kiểm tra kho")
        print("[Order Tracking Service] Mã đơn hàng:", order_id)
        print("[Order Tracking Service] Trạng thái kho:", inventory_status)
        print("[Order Tracking Service] Lý do:", reason)

        update_order_history(
            order_id,
            "Inventory Service",
            inventory_status,
            reason
        )

        write_log(
            order_id,
            "Inventory Service",
            inventory_status,
            reason
        )

        print_order_history(order_id)


def handle_payment_result(payment_result):
    order_id = payment_result["order_id"]
    payment_status = payment_result["payment_status"]
    reason = payment_result["reason"]

    with lock:
        print("\n" + "=" * 60)
        print("[Order Tracking Service] Nhận kết quả thanh toán")
        print("[Order Tracking Service] Mã đơn hàng:", order_id)
        print("[Order Tracking Service] Trạng thái thanh toán:", payment_status)
        print("[Order Tracking Service] Lý do:", reason)

        update_order_history(
            order_id,
            "Payment Service",
            payment_status,
            reason
        )

        write_log(
            order_id,
            "Payment Service",
            payment_status,
            reason
        )

        print_order_history(order_id)


def inventory_callback(message):
    try:
        data = message.data.decode("utf-8")
        inventory_result = json.loads(data)

        if "order_id" not in inventory_result or "inventory_status" not in inventory_result:
            print("[Order Tracking Service] Bỏ qua tin nhắn kiểm kho không đúng định dạng")
            message.ack()
            return

        handle_inventory_result(inventory_result)

        message.ack()
        print("[Order Tracking Service] Đã xác nhận tin nhắn kiểm kho")

    except Exception as e:
        print("[Order Tracking Service] Lỗi khi theo dõi kiểm kho:", e)
        message.nack()


def payment_callback(message):
    try:
        data = message.data.decode("utf-8")
        payment_result = json.loads(data)

        if "order_id" not in payment_result or "payment_status" not in payment_result:
            message.ack()
            return

        handle_payment_result(payment_result)

        message.ack()
        print("[Order Tracking Service] Đã xác nhận tin nhắn thanh toán")

    except Exception as e:
        print("[Order Tracking Service] Lỗi khi theo dõi thanh toán:", e)
        message.nack()


print("[Order Tracking Service] Đang theo dõi trạng thái xử lý đơn hàng...")
print("[Order Tracking Service] Subscription kiểm kho:", inventory_tracking_subscription_path)
print("[Order Tracking Service] Subscription thanh toán:", payment_tracking_subscription_path)
print("[Order Tracking Service] File log:", file_log)


inventory_streaming_pull_future = subscriber.subscribe(
    inventory_tracking_subscription_path,
    callback=inventory_callback,
    flow_control=flow_control
)

payment_streaming_pull_future = subscriber.subscribe(
    payment_tracking_subscription_path,
    callback=payment_callback,
    flow_control=flow_control
)


try:
    while True:
        time.sleep(60)

except KeyboardInterrupt:
    inventory_streaming_pull_future.cancel()
    payment_streaming_pull_future.cancel()
    print("\n[Order Tracking Service] Đã dừng")