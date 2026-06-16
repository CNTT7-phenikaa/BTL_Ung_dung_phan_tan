import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from google.cloud import pubsub_v1
from config import project_id,inventory_result_sub_id, payment_result_topic_id

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
flow_control = pubsub_v1.types.FlowControl(max_messages=1)

inventory_result_subscription_path = subscriber.subscription_path(
    project_id,
    inventory_result_sub_id
)
payment_result_topic_path = publisher.topic_path(
    project_id,
    payment_result_topic_id
)

inventory_stock = {
    "Laptop": 1,
    "Mouse": 10,
    "Keyboard": 5
}


def print_inventory_stock():
    print("[Payment Service] Số lượng hàng còn lại trong kho:")
    for product, quantity in inventory_stock.items():
        print("[Payment Service]", product + ":", quantity)


def update_inventory_after_success(items):
    print("[Payment Service] Thanh toán thành công, tiến hành trừ kho...")

    for item in items:
        product = item["product"]
        quantity = item["quantity"]

        if product in inventory_stock:
            inventory_stock[product] -= quantity

            print(
                "[Payment Service] Đã trừ kho:",
                product,
                "| Số lượng:",
                quantity
            )

    print_inventory_stock()

def calculate_total_amount(items):
    total_amount = 0

    for item in items:
        total_amount += item["quantity"] * item["price"]

    return total_amount

def process_payment(inventory_result):
    order_id = inventory_result["order_id"]
    customer = inventory_result["customer"]
    items = inventory_result["items"]
    inventory_status = inventory_result["inventory_status"]

    total_amount = calculate_total_amount(items)
    
    print("\n" + "=" * 60)
    print("\n[Payment Service] Đã nhận được kết quả kiểm tra kho hàng")
    print("[Payment Service] Order ID:", order_id)
    print("[Payment Service] Customer:", customer)
    print("[Payment Service] Trạng thái kho hàng:", inventory_status)
    print("[Payment Service] Items:")

    for item in items:
        print(
            "[Payment Service]",
            item["product"],
            "| Quantity:",
            item["quantity"],
            "| Price:",
            item["price"],
            "VND"
        )

    print("[Payment Service] Total amount:", total_amount, "VND")
    if inventory_status == "Không đủ hàng":
        payment_status = "Không thể thực hiện thanh toán"
        reason = "Không đủ sản phẩm trong kho"
        print("[Payment Service] Đơn hàng:", order_id)
        print("[Payment Service] Trạng thái:", payment_status)
        print("[Payment Service] Lý do:", reason)
    elif inventory_status == "Xác nhận đủ hàng trong kho":
        
        print("[Payment Service] Đang xử lý thanh toán...")
        time.sleep(2)
        
        
        if total_amount > 15000000:
            payment_status = "Thanh toán thất bại"
            reason = "Tài khoản của quý khách không đủ để thực hiện giao dịch"
            print(f"[Payment Service] Đơn hàng {order_id} thanh toán không thành công")
            print(f"[Payment Service] Lý do: {reason}")
            print(f"Trạng thái: {payment_status}")
        else:
            payment_status = "Thanh toán thành công"
            reason = "Đơn hàng đã được thanh toán"

            print(f"[Payment Service] Đơn hàng {order_id} thanh toán thành công")
            print(f"[Payment Service] Lý do: {reason}")
            print(f"[Payment Service] Trạng thái: {payment_status}")

            update_inventory_after_success(items)
    else:
        payment_status = "Không thể thanh toán đơn hàng" 
        reason = "Không xác định được số hàng trong kho"
        print("[Payment Service] Trạng thái hàng trong kho: ", inventory_status)
        print("[Payment Service] Trạng thái thanh toán: ", payment_status)
    payment_result = {
        "order_id": order_id,
        "customer": customer,
        "items": items,
        "total_amount": total_amount,
        "inventory_status": inventory_status,
        "payment_status": payment_status,
        "reason": reason,
        "sp_thieu": inventory_result.get("sp_thieu", [])
    }

    return payment_result
def publish_payment_result(payment_result):
    data = json.dumps(payment_result).encode("utf-8")

    future = publisher.publish(payment_result_topic_path, data)
    message_id = future.result()

    print("[Payment Service] Gửi kết quả thanh toán")
    print("[Payment Service] Result Message ID:", message_id)


def callback(message):
    try:
        data = message.data.decode("utf-8")
        inventory_result = json.loads(data)

        if "items" not in inventory_result or "inventory_status" not in inventory_result:
            print("[Payment Service] Bỏ qua tin nhắn không đúng định dạng kết quả kiểm kho")
            message.ack()
            return

        payment_result = process_payment(inventory_result)
        publish_payment_result(payment_result)

        message.ack()
        print("[Payment Service] Xác nhận tin nhắn")

    except Exception as e:
        print("[Payment Service] Lỗi:", e)
        message.nack()


print("[Payment Service] Chờ yêu cầu thanh toán mới...")
print("[Payment Service] Subscription:", inventory_result_subscription_path)
print("[Payment Service] Payment result topic:", payment_result_topic_path)

streaming_pull_future = subscriber.subscribe(
    inventory_result_subscription_path,
    callback=callback,
    flow_control= flow_control
)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\n[Payment Service] Stopped")