import pandas as pd
from scipy import sparse
from sklearn.metrics.pairwise import cosine_similarity
import math
import numpy as np

fileName = "isFollow"
mangas_df = pd.read_csv('../Collaborative Filtering/mangas.csv')
behaviors_df = pd.read_csv(f'../Collaborative Filtering/behaviors_{fileName}.csv')

if fileName == "isFollow":
    behaviors_df['isFollow'] = behaviors_df['isFollow'].replace({True: 5, False: 0}).infer_objects(copy=False)

behaviors_df[fileName].fillna(0, inplace=True)
user_id_list = behaviors_df['userId'].unique()
manga_id_list = mangas_df['_id'].unique()

# expanded_data = []
# for user_id in user_id_list:
#     for manga_id in manga_id_list:
#         expanded_data.append({'userId': user_id, '_id': manga_id})
# expanded_df = pd.DataFrame(expanded_data)
# expanded_df.rename(columns={'_id': 'mangaId'}, inplace=True)
expanded_data = [{'userId': user_id, '_id': manga_id} for user_id in user_id_list for manga_id in manga_id_list]
expanded_df = pd.DataFrame(expanded_data)
expanded_df.rename(columns={'_id': 'mangaId'}, inplace=True)

merged_df = pd.merge(expanded_df, behaviors_df, on=['userId', 'mangaId'], how='left')
merged_df[fileName] = merged_df[fileName].fillna(0)
merged_df.drop(columns=['updatedAt'], inplace=True)

mangas_df.rename(columns={'_id': 'mangaId'}, inplace=True)
datas = pd.merge(mangas_df, merged_df, on='mangaId').drop(['author','genre'],axis=1)

userDatas = datas.pivot_table(index=['userId'],columns=['mangaId'],values=fileName)

# def standardize(row):
#     new_row = (row - row.mean())/(row.max()-row.min())
#     return new_row
# df_std = userDatas.apply(standardize).T
def standardize(row):
    return (row - row.mean()) / (row.max() - row.min())
df_std = userDatas.apply(standardize).T.fillna(0)

# df_std.fillna(0, inplace=True)
# sparse_df = sparse.csr_matrix(df_std.values)
# corrMatrix = pd.DataFrame(cosine_similarity(sparse_df),index=userDatas.columns,columns=userDatas.columns)
sparse_df = sparse.csr_matrix(df_std.values)
cosine_sim_matrix = pd.DataFrame(cosine_similarity(sparse_df), index=userDatas.columns, columns=userDatas.columns)

corrMatrix = userDatas.corr(method='pearson')

# def get_recommendations(user_id, userViews, corrMatrix, num_recommendations=36):
#     user_data = userViews.loc[user_id]
#     watched_items = user_data[user_data > 0].index.tolist()

#     recommendations = {}

#     for item in watched_items:
#         similar_items = corrMatrix[item].sort_values(ascending=False)

#         for similar_item, score in similar_items.items():
#             if similar_item in recommendations:
#                 recommendations[similar_item] += score
#             else:
#                 recommendations[similar_item] = score

#         if item in recommendations:
#             recommendations[item] += 1 
#         else:
#             recommendations[item] = 1 

#     sorted_recommendations = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)

#     return sorted_recommendations[:num_recommendations]
def get_recommendations(user_id, userViews, corrMatrix, num_recommendations=36):
    user_data = userViews.loc[user_id]
    watched_items = user_data[user_data > 0].index.tolist()

    recommendations = {}
    for item in watched_items:
        similar_items = corrMatrix[item].drop(item).sort_values(ascending=False)
        for similar_item, score in similar_items.items():
            if np.isnan(score):  # Kiểm tra và loại bỏ giá trị NaN
                continue
            if similar_item in recommendations:
                recommendations[similar_item] += score
            else:
                recommendations[similar_item] = score

    for item in watched_items:
        if item in recommendations:
            recommendations[item] += 1.0
        else:
            recommendations[item] = 1.0 

    sorted_recommendations = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)
    return sorted_recommendations[:num_recommendations]

user_id = '5f892400948be104b0830fde'
recommendations = get_recommendations(user_id, userDatas, corrMatrix)

# filtered_recommendations = [item_id for item_id, score in recommendations if not math.isnan(score)]
filtered_recommendations = [item_id for item_id, score in recommendations if not np.isnan(score)]
