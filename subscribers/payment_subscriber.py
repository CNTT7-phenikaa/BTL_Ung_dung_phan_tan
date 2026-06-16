import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from google.cloud import pubsub_v1
from config import project_id, payment_sub_id, payment_result_topic_id

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()


payment_subscription_path = subscriber.subscription_path(project_id, payment_sub_id)
payment_result_topic_path = publisher.topic_path(project_id, payment_result_topic_id)

def process_payment(order):
    order_id = order["order_id"]
    customer = order["customer"]
    product = order["product"]
    quantity = order["quantity"]
    price = order["price"]

    total_amount = quantity * price

    print("\n[Payment Service] Received order")
    print("[Payment Service] Order ID:", order_id)
    print("[Payment Service] Customer:", customer)
    print("[Payment Service] Product:", product)
    print("[Payment Service] Quantity:", quantity)
    print("[Payment Service] Total amount:", total_amount, "VND")

    print("[Payment Service] Đang xử lý thanh toán...")
    time.sleep(5)
    
    
    if total_amount > 10000000:
        payment_status = "Thanh toán thất bại"
        reason = "Tài khoản của quý khách không đủ để thực hiện giao dịch"
        print(f"[Payment Service] Đơn hàng {order_id} thanh toán không thành công")
        print(f"[Payment Service] Lý do: {reason}")
        print(f"Trạng thái: {payment_status}")
    else:
        payment_status = "Thanh toán thành công"
        reason = "Đơn hàng đã được thanh toán"
        print(f"[Payment Service] Dơn hành {order_id} thanh toán thành công")
        print(f"Lý do: {reason}")
        print(f"[Payment Service] Trạng thái: {payment_status}")
        
    payment_result = {
        "order_id": order_id,
        "customer": customer,
        "product": product,
        "quantity": quantity,
        "total_amount": total_amount,
        "payment_status": payment_status,
        "reason": reason
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
        order = json.loads(data)

        payment_result = process_payment(order)
        publish_payment_result(payment_result)

        message.ack()
        print("[Payment Service] Xác nhận tin nhắn")

    except Exception as e:
        print("[Payment Service] Error:", e)
        message.nack()


print("[Payment Service] Listening for messages...")
print("[Payment Service] Subscription:", payment_subscription_path)
print("[Payment Service] Payment result topic:", payment_result_topic_path)

streaming_pull_future = subscriber.subscribe(
    payment_subscription_path,
    callback=callback
)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("\n[Payment Service] Stopped")