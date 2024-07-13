import csv
import unicodedata
import re
import os
from pymongo import MongoClient
from config.db import connect_to_mongodb
import pandas as pd
from bson.objectid import ObjectId

db = connect_to_mongodb()

collection_name = "behaviors"

output_folder = "Collaborative Filtering"
os.makedirs(output_folder, exist_ok=True)

def export_data_to_csv(field_name):
    csv_file_path = os.path.join(output_folder, f"{collection_name}_{field_name}.csv")
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)
        # print(f"Đã xóa tập tin CSV cũ: {csv_file_path}")

    # Xóa file CSV cũ nếu tồn tại
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)

    try:
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['userId', 'mangaId', field_name, 'updatedAt'])

            # Lấy tất cả các behaviors từ MongoDB
            behaviors = db.behaviors.find()

            for behavior in behaviors:
                user_id = str(behavior['userId'])
                for entry in behavior['behaviorList']:
                    manga_id = str(entry['mangaId'])
                    value = entry.get(field_name)  # Lấy giá trị của trường dữ liệu chính
                    updated_at = entry['updatedAt']
                    writer.writerow([user_id, manga_id, value, updated_at])

        print(f"Dữ liệu được xuất ra {csv_file_path}")

    except Exception as e:
        print(f"Xuất dữ liệu thất bại: {e}")

def export_behavior_to_csv():
    # Tên các trường dữ liệu chính
    fields = ['rating', 'numOfComment', 'isFollow', 'sumTimeRead', 'readingFrequency', 'view']

    # Xuất dữ liệu cho từng trường dữ liệu chính
    for field in fields:
        export_data_to_csv(field)

def export_data_to_dict(field_name):
    try:
        data = []

        # Lấy tất cả các behaviors từ MongoDB
        behaviors = db.behaviors.find()
        
        for behavior in behaviors:
            user_id = str(behavior['userId'])
            for entry in behavior['behaviorList']:
                manga_id = str(entry['mangaId'])
                value = entry.get(field_name) 
                updated_at = entry['updatedAt']
                data.append({
                    'userId': user_id,
                    'mangaId': manga_id,
                    field_name: value,
                    'updatedAt': updated_at
                })
        df = pd.DataFrame(data, columns=['userId', 'mangaId', field_name, 'updatedAt'])
        return df

    except Exception as e:
        print(f"Xuất dữ liệu thất bại: {e}")
        return pd.DataFrame(columns=['userId', 'mangaId', field_name, 'updatedAt'])


def export_behavior_by_id(userId):
    try:
        user_id = ObjectId(userId)
        user_data = db.behaviors.find_one({'userId': user_id})
        if user_data and 'behaviorList' in user_data:
            # Trích xuất danh sách hành vi
            behavior_list = user_data['behaviorList']
            
            # Tạo mảng object chỉ chứa mangaId và rating dưới dạng tuple
            manga_rating_list = []
            for behavior in behavior_list:
                mangaId = str(behavior['mangaId'])
                rating = behavior.get('rating', None)
                if rating is not None:
                    manga_rating_list.append((mangaId, rating))
                
            return manga_rating_list
        else:
            return []

    except Exception as e:
        print(f"Xuất dữ liệu thất bại: {e}")
        return []

# CSV output file path
# csv_file_path = os.path.join("Collaborative Filtering" ,f"{collection_name}.csv")

# def export_behavior_to_csv():
#     if os.path.exists(csv_file_path):
#         os.remove(csv_file_path)
#         print(f"Đã xóa tập tin CSV cũ: {csv_file_path}")
#     collection = db[collection_name]

#     try:
#         # Lấy tất cả các tài liệu từ bộ sưu tập
#         documents = list(collection.find({}))

#         if not documents:
#             print("Không tìm thấy tài liệu nào trong bộ sưu tập.")
#             return

#         # Trích xuất tên trường (keys) từ tài liệu đầu tiên
#         field_names = list(documents[0].keys())
#         print("Tên trường:", field_names)

#         # In ra các tài liệu mẫu để kiểm tra
#         print("Các tài liệu mẫu:")
#         for doc in documents[:5]:  # Chỉ in ra 5 tài liệu đầu tiên
#             print(doc)

#         # Ghi các tài liệu vào tập tin CSV
#         with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
#             writer = csv.DictWriter(csv_file, fieldnames=field_names)
#             writer.writeheader()
#             writer.writerows(documents)

#         print(f"Dữ liệu được xuất ra {csv_file_path}")

#     except Exception as e:
#         print(f"Xuất dữ liệu thất bại: {e}")


# # Gọi hàm xuất dữ liệu
# export_behavior_to_csv()