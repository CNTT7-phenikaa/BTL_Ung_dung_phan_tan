# Kiến trúc hệ thống xử lý đơn hàng phân tán

## 1. Tổng quan kiến trúc

Dự án mô phỏng hệ thống xử lý đơn hàng trong thương mại điện tử theo mô hình ứng dụng phân tán. Thay vì xử lý toàn bộ quy trình trong một chương trình duy nhất, hệ thống được chia thành nhiều service độc lập, mỗi service đảm nhận một chức năng riêng.

Các service trong hệ thống giao tiếp với nhau thông qua Google Cloud Pub/Sub. Cơ chế này giúp các thành phần xử lý đơn hàng hoạt động bất đồng bộ, giảm sự phụ thuộc trực tiếp giữa các service và giúp hệ thống dễ mở rộng hơn.

Kiến trúc tổng quát của hệ thống bao gồm:
    - Order Publisher
    - Inventory Service
    - Email Service
    - Analytics Service
    - Payment Service
    - Payment Notification Service
    - Order Tracking Service
    - Google Cloud Pub/Sub Topics và Subscriptions

## 2. Các thành phần trong hệ thống

### 2.1. Order Publisher

`order_publisher.py` là thành phần tạo đơn hàng mới. Sau khi tạo dữ liệu đơn hàng, chương trình sẽ đóng gói toàn bộ gói tin dưới dạng JSON và gửi message lên `order-topic`. Đây là điểm bắt đầu của toàn bộ quy trình xử lý đơn hàng.

### 2.2. Inventory Service

`inventory_subscriber.py` nhận đơn hàng từ `order-topic` thông qua `subscription inventory-sub`.

Service này có nhiệm vụ kiểm tra số lượng sản phẩm trong kho. Nếu các sản phẩm trong đơn hàng còn đủ số lượng, Inventory Service sẽ gửi kết quả kiểm kho thành công sang `inventory-result-topic`. Nếu không đủ hàng, hệ thống sẽ thông báo đơn hàng không thể tiếp tục xử lý thanh toán.

Chức năng này giúp tránh tình trạng khách hàng thanh toán thành công nhưng sản phẩm không còn trong kho.

### 2.3. Email Service

`email_subscriber.py` nhận đơn hàng từ `order-topic` thông qua subscription `email-sub`.

Service này có nhiệm vụ mô phỏng việc gửi email thông báo tiếp nhận đơn hàng cho khách hàng. Khi khách hàng tạo đơn hàng thành công, Email Service sẽ hiển thị thông tin xác nhận đơn hàng trên terminal.

### 2.4. Analytics Service

`analytics_subscriber.py` nhận đơn hàng từ `order-topic` thông qua `subscription analytics-sub`.

Service này dùng để phân tích thông tin đơn hàng, ví dụ như tính tổng giá trị đơn hàng và phân loại đơn hàng theo giá trị. Thành phần này mô phỏng chức năng thống kê và hỗ trợ phân tích dữ liệu trong hệ thống thương mại điện tử.

### 2.5. Payment Service 

`payment_subscriber.py` không nhận trực tiếp đơn hàng từ `order-topic`, mà nhận kết quả kiểm kho từ `inventory-result-topic` thông qua subscription `inventory-result-sub`.

Payment Service chỉ xử lý thanh toán khi đơn hàng đã được Inventory Service xác nhận đủ hàng. Sau khi xử lý thanh toán, service sẽ gửi kết quả sang `payment-result-topic`. Nếu khách hàng thanh toán thành công, số lượng hàng trong kho mới bị trừ.

Cách thiết kế này giúp quy trình xử lý đơn hàng hợp lý hơn, vì hệ thống chỉ thanh toán khi hàng trong kho còn đủ.

### 2.6. Payment Notification Service

`payment_notification_subscriber.py` nhận kết quả thanh toán từ `payment-result-topic` thông qua subscription `payment-result-sub`.

Service này có nhiệm vụ thông báo kết quả thanh toán cuối cùng cho khách hàng. Nếu thanh toán thành công, hệ thống hiển thị thông báo thanh toán thành công. Nếu thanh toán thất bại, hệ thống hiển thị thông báo thất bại.

### 2.7. Order Tracking Service

`order_tracking_subscriber.py` là service dùng để theo dõi trạng thái xử lý đơn hàng.

Service này lắng nghe kết quả từ cả hai topic:

    - inventory-result-topic
    - payment-result-topic

Thông qua các subscription riêng:

    - tracking-inventory-result-sub
    - tracking-payment-result-sub

Sau khi nhận được message, service sẽ ghi trạng thái xử lý vào file logs/order_tracking.log.

Chức năng này giúp hệ thống có thể theo dõi lịch sử xử lý đơn hàng, hỗ trợ kiểm tra lỗi và đối chiếu trạng thái trong quá trình demo hoặc vận hành.

## 3. Danh sách topic và subscription

| Topic | Subscription | Service sử dụng | Vai trò |
|---|---|---|---|
| order-topic | inventory-sub | inventory_subscriber.py | Nhận đơn hàng để kiểm tra tồn kho |
| order-topic | email-sub | email_subscriber.py | Nhận đơn hàng để gửi thông báo tiếp nhận |
| order-topic | analytics-sub | analytics_subscriber.py | Nhận đơn hàng để phân tích/thống kê |
| inventory-result-topic | inventory-result-sub | payment_subscriber.py | Nhận kết quả kiểm kho để xử lý thanh toán |
| inventory-result-topic | tracking-inventory-result-sub | order_tracking_subscriber.py | Theo dõi kết quả kiểm kho |
| payment-result-topic | payment-result-sub | payment_notification_subscriber.py | Nhận kết quả thanh toán để gửi thông báo |
| payment-result-topic | tracking-payment-result-sub | order_tracking_subscriber.py | Theo dõi kết quả thanh toán |

## 4. Luồng xử lý đơn hàng

Quy trình xử lý đơn hàng trong hệ thống diễn ra như sau:

1. `order_publisher.py`tạo đơn hàng mới.
2. Đơn hàng được gửi lên `order-topic`.
3. Từ `order-topic`, nhiều service có thể nhận cùng một đơn hàng thông qua các subscription khác nhau.
4. `inventory_subscriber.py` kiểm tra tồn kho.
5. Nếu đơn hàng đủ hàng, kết quả kiểm kho được gửi sang `inventory-result-topic`.
6. `payment_subscriber.py` nhận kết quả kiểm kho và xử lý thanh toán.
7. Sau khi thanh toán, kết quả được gửi sang `payment-result-topic`.
8. `payment_notification_subscriber.py` nhận kết quả thanh toán và thông báo cho khách hàng.
9. `order_tracking_subscriber.py` theo dõi kết quả kiểm kho và kết quả thanh toán, sau đó ghi trạng thái vào file log.
10. `email_subscriber.py` và `analytics_subscriber.py` xử lý song song các tác vụ phụ như gửi thông báo tiếp nhận đơn hàng và phân tích đơn hàng.

## 5. Sơ đồ luồng xử lý

![Kiến trúc hệ thống](Kien_truc_dat_hang_final.drawio.png)


## 6. Ý nghĩa của kiến trúc phân tán

Kiến trúc sử dụng Google Cloud Pub/Sub mang lại một số lợi ích cho hệ thống:

Thứ nhất, các service được tách rời với nhau. Order Publisher không cần gọi trực tiếp Inventory Service, Email Service hay Analytics Service. Nó chỉ cần gửi message lên topic, sau đó các subscriber tự nhận và xử lý.

Thứ hai, hệ thống xử lý bất đồng bộ. Các service không cần chờ nhau hoàn thành toàn bộ quy trình. Ví dụ, Email Service và Analytics Service có thể xử lý song song với Inventory Service.

Thứ ba, hệ thống dễ mở rộng. Nếu muốn bổ sung chức năng mới, nhóm có thể tạo thêm subscription hoặc service mới để lắng nghe topic hiện có mà không cần sửa nhiều vào các service cũ.

Thứ tư, hệ thống tăng khả năng chịu lỗi. Nếu một subscriber tạm thời dừng hoạt động, message vẫn có thể được lưu trong subscription và xử lý lại khi service hoạt động trở lại.

## 7. Các chức năng mới trong kiến trúc

Trong kiến trúc hiện tại, nhóm phát triển ba chức năng mở rộng chính:

Chức năng thứ nhất là kiểm tra tồn kho trước khi thanh toán. Đây là chức năng do Inventory Service đảm nhiệm. Hệ thống chỉ chuyển sang bước thanh toán nếu đơn hàng đã được xác nhận đủ hàng.

Chức năng thứ hai là gửi thông báo sau khi xử lý thanh toán. Đây là chức năng do Payment Notification Service đảm nhiệm. Service này nhận kết quả thanh toán từ payment-result-topic và hiển thị thông báo cuối cùng cho khách hàng.

Chức năng thứ ba là theo dõi đơn hàng và ghi log. Đây là chức năng do Order Tracking Service đảm nhiệm. Service này theo dõi cả kết quả kiểm kho và kết quả thanh toán, sau đó ghi lại trạng thái ra terminal và vào file log.

## 8. Kết luận

Kiến trúc của hệ thống được thiết kế theo hướng phân tán, trong đó mỗi service đảm nhiệm một nhiệm vụ riêng và giao tiếp với nhau thông qua Google Cloud Pub/Sub. Cách thiết kế này giúp hệ thống dễ mở rộng, dễ bảo trì và phù hợp với mô hình xử lý đơn hàng trong thực tế.

Thông qua dự án, nhóm đã mô phỏng được quy trình xử lý đơn hàng gồm tạo đơn, kiểm tra tồn kho, xử lý thanh toán, gửi thông báo, phân tích đơn hàng và theo dõi trạng thái xử lý.
