import os
import csv
from config.db import connect_to_mongodb

collection_name = "genres"
csv_file_path = os.path.join("Collaborative Filtering", f"{collection_name}.csv")

desired_fields = ["name", "nameOnUrl"]

db = connect_to_mongodb()

def export_mangas_to_csv():
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)
        print(f"Đã xóa tập tin CSV cũ: {csv_file_path}")
    collection = db[collection_name]

    try:
        documents = list(collection.find({}))

        if not documents:
            print("Không tìm thấy tài liệu nào trong collection.")
            return

        # Ghi tài liệu vào tập tin CSV
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=desired_fields)
            writer.writeheader()

            for doc in documents:
                # Tạo một dictionary chỉ chứa các trường mong muốn từ mỗi tài liệu
                filtered_doc = {field: doc.get(field, "") for field in desired_fields}
                writer.writerow(filtered_doc)

        print(f"Dữ liệu được xuất ra {csv_file_path}")

    except Exception as e:
        print(f"Xuất dữ liệu thất bại: {e}")

    finally:
        # Đóng kết nối tới MongoDB
        # client.close()
        print("Ngắt kết nối với MongoDB")

# Gọi hàm để xuất dữ liệu
export_mangas_to_csv()
