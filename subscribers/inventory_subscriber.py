import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from google.cloud import pubsub_v1
from config import project_id, inventory_sub_id, inventory_result_topic_id


subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
flow_control = pubsub_v1.types.FlowControl(max_messages=1)

inventory_subscription_path = subscriber.subscription_path(project_id, inventory_sub_id)
inventory_result_topic_path = publisher.topic_path(project_id, inventory_result_topic_id)


inventory_stock = {
    "Laptop": 1,
    "Mouse": 10,
    "Keyboard": 5
}


def check_inventory(order):
    order_id = order["order_id"]
    customer = order["customer"]
    items = order["items"]

    

    print("\n" + "=" * 60)
    print("[Inventory Service] Tiếp nhận đơn hàng")
    print("[Inventory Service] Order ID:", order_id)
    print("[Inventory Service] Customer:", customer)
    print("[Inventory Service] Kiểm tra giỏ hàng:")
    
    sp_thieu = []
    for item in items:
        product = item["product"]
        quantity = item["quantity"]
        available_quantity = inventory_stock.get(product, 0)
        
        print("[Inventory Service]",
            product,
            "| Yêu cầu:",
            quantity,
            "| Tồn kho:",
            available_quantity
        )
        if available_quantity < quantity:
            sp_thieu.append({
                "product": product,
                "requested_quantity": quantity,
                "available_quantity": available_quantity
        })
    print("[Inventory Service] Đang kiểm tra số lượng trong kho...")
    time.sleep(2)

    if len(sp_thieu) == 0:
        inventory_status = "Xác nhận đủ hàng trong kho"
        reason = "Số lượng trong kho còn đủ, chờ xử lý thanh toán"
        print("[Inventory Service] Đơn hàng:", order_id)
        print("[Inventory Service] Trạng thái:", inventory_status)
        print("[Inventory Service] Số lượng hàng còn trong kho:")
        for product, quantity in inventory_stock.items():
            print("[Inventory Service]", product + ":", quantity)

    else:
        inventory_status = "Không đủ hàng"
        reason = "Một số sản phẩm không đủ lượng hàng trong kho"

        print("[Inventory Service] Đơn hàng:", order_id)
        print("[Inventory Service] Trạng thái:", inventory_status)
        print("[Inventory Service] Lý do:", reason)
        for item in sp_thieu:
            print(
                "[Inventory Service]",
                item["product"],
                "| Requested:",
                item["requested_quantity"],
                "| Available:",
                item["available_quantity"]
            )
    inventory_result = {
        "order_id": order_id,
        "customer": customer,
        "items": items,
        "inventory_status": inventory_status,
        "reason": reason,
        "sp_thieu": sp_thieu
    }

    return inventory_result


def publish_inventory_result(inventory_result):
    data = json.dumps(inventory_result).encode("utf-8")

    future = publisher.publish(inventory_result_topic_path, data)
    message_id = future.result()

    print("[Inventory Service] Kết quả kiểm tra kho hàng")
    print("[Inventory Service] Result Message ID:", message_id)


def callback(message):
    try:
        data = message.data.decode("utf-8")
        order = json.loads(data)

        inventory_result = check_inventory(order)

        publish_inventory_result(inventory_result)

        message.ack()
        print("[Inventory Service] Xác nhận kiểm tra đơn đặt hàng")

    except Exception as e:
        print("[Inventory Service] Error:", e)
        message.nack()


print("[Inventory Service] Đang chờ đơn hàng mới...")
print("[Inventory Service] Subscription:", inventory_subscription_path)
print("[Inventory Service] Inventory result topic:", inventory_result_topic_path)


streaming_pull_future = subscriber.subscribe(
    inventory_subscription_path,
    callback=callback,
    flow_control=flow_control
)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\n[Inventory Service] Stopped")