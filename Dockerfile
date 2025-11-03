# Sử dụng một image Python chính thức làm nền
FROM python:3.9-slim

# Cài đặt Java (cần thiết cho PySpark)
RUN apt-get update && apt-get install -y default-jdk

# Đặt thư mục làm việc trong container
WORKDIR /code

# Sao chép file requirements.txt vào trước để tận dụng cache
COPY requirements.txt .

# Cài đặt các thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ code của bạn vào container
COPY . .

# Mở cổng 7860 để ứng dụng có thể truy cập từ bên ngoài
# Hugging Face Spaces mặc định sử dụng cổng này
EXPOSE 7860

# Lệnh để chạy ứng dụng của bạn khi container khởi động
# Chúng ta dùng gunicorn để chạy ổn định hơn
CMD ["gunicorn", "--bind", "0.0.0.0:7860", "--timeout", "600", "app:app"]
