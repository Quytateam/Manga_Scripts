import csv
import unicodedata
import re
import os
from config.db import connect_to_mongodb
from export_behavior import export_behavior_to_csv
import pandas as pd

collection_name = "mangas"

db = connect_to_mongodb()

# CSV output file path
csv_file_path = os.path.join("Collaborative Filtering" ,f"{collection_name}.csv")

desired_fields = ["_id", "name", "author", "genre"]

def normalize_text(text):
    normalized_text = unicodedata.normalize('NFD', text.lower()).encode('ascii', 'ignore').decode('utf-8')
    normalized_text = re.sub(r'\s+', '-', normalized_text)
    normalized_text = re.sub(r'[^a-z0-9\-]', '', normalized_text)
    return normalized_text

def format_genre(genre_list):
    if isinstance(genre_list, list):
        normalized_genres = [normalize_text(genre) for genre in genre_list]
        return '|'.join(normalized_genres)
    return ""

def export_mangas_to_csv():
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)
        print(f"Đã xóa tập tin CSV cũ: {csv_file_path}")
    # Kết nối tới MongoDB
    # client = MongoClient(uri)
    # db = client[database_name]
    collection = db[collection_name]

    try:
        # Lấy tất cả các tài liệu từ collection
        documents = list(collection.find({}))

        if not documents:
            print("Không tìm thấy tài liệu nào trong collection.")
            return

        # Ghi tài liệu vào tập tin CSV
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=desired_fields)
            writer.writeheader()

            for doc in documents:
                # Chuyển đổi giá trị của trường 'genre' thành chuỗi được định dạng mong muốn
                doc["genre"] = format_genre(doc.get("genre", []))

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

def export_mangas_to_dict():
    collection = db[collection_name]

    try:
        # Lấy tất cả các tài liệu từ collection
        documents = list(collection.find({}))

        if not documents:
            print("Không tìm thấy tài liệu nào trong collection.")
            return []

        # Chuẩn bị dữ liệu để trả về
        result = []
        for doc in documents:
            # Chuyển đổi giá trị của trường 'genre' thành chuỗi được định dạng mong muốn
            doc["_id"] = str(doc.get("_id"))
            doc["genre"] = format_genre(doc.get("genre", []))

            # Tạo một dictionary chỉ chứa các trường mong muốn từ mỗi tài liệu
            filtered_doc = {field: doc.get(field, "") for field in desired_fields}
            result.append(filtered_doc)
        df = pd.DataFrame(result, columns=desired_fields)
        return df

    except Exception as e:
        print(f"Xuất dữ liệu thất bại: {e}")
        return pd.DataFrame(columns=desired_fields)

    finally:
        # Đóng kết nối tới MongoDB
        print("Ngắt kết nối với MongoDB")

# Gọi hàm xuất dữ liệu

def export_datas():
    try:
        export_mangas_to_csv()
        export_behavior_to_csv()
    except Exception as e:
        print(f'Error: {e}')