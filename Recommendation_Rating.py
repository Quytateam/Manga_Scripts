import os
import pandas as pd
import random
from scipy import sparse
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from export_manga import export_mangas_to_dict
from export_behavior import export_data_to_dict,export_behavior_by_id
from config.db import connect_to_mongodb
from pymongo import ReturnDocument
from bson.objectid import ObjectId
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

db = connect_to_mongodb()

def setRatingList(rating):
    fileName = "rating"
    # mangas_df = pd.read_csv('./Collaborative Filtering/mangas.csv')
    # behaviors_df = pd.read_csv(f'./Collaborative Filtering/behaviors_{fileName}.csv')
    mangas_df = export_mangas_to_dict()
    behaviors_df = export_data_to_dict(fileName)
    
    behaviors_df[fileName].fillna(0, inplace=True)

    user_id_list = behaviors_df['userId'].unique()
    manga_id_list = mangas_df['_id'].unique()
    expanded_data = []

    for user_id in user_id_list:
        for manga_id in manga_id_list:
            expanded_data.append({'userId': user_id, '_id': str(manga_id)})

    expanded_df = pd.DataFrame(expanded_data)
    
    expanded_df.rename(columns={'_id': 'mangaId'}, inplace=True)
    merged_df = pd.merge(expanded_df, behaviors_df, on=['userId', 'mangaId'], how='left')
    
    merged_df[fileName] = merged_df[fileName].fillna(0)
    merged_df.drop(columns=['updatedAt'], inplace=True)
    
    mangas_df.rename(columns={'_id': 'mangaId'}, inplace=True)
    datas = pd.merge(mangas_df, merged_df, on='mangaId').drop(['author','genre'],axis=1)
    
    userDatas = datas.pivot_table(index=['userId'],columns=['mangaId'],values=fileName)
    # Thresh là giá trị yếu cầu tối thiểu bao nhiêu cột (thresh=10)
    userDatas = userDatas.dropna(thresh=1, axis=1).fillna(0,axis=1)
    #userRatings.fillna(0, inplace=True)
    
    corrMatrix = userDatas.corr(method='pearson',min_periods=1)
    
    def get_similar(manga_Id,data):
        similar_datas = corrMatrix[manga_Id]*(data-2.5)
        # similar_datas = corrMatrix[manga_Id]*(data)
        similar_datas = similar_datas.sort_values(ascending=False)
        #print(type(similar_ratings))
        return similar_datas
    
    # Chính
    # listBehavior = export_behavior_by_id(rating.userId)
    # similar_mangas_list = []
    # for mangaId,rate in listBehavior:
    #     similar_mangas_list.append(get_similar(mangaId, rate))

    # similar_mangas = pd.concat(similar_mangas_list, axis=1).sum(axis=1).reset_index()
    
    # similar_mangas = pd.concat(similar_mangas_list, axis=1)
    # similar_mangas.reset_index(drop=True, inplace=True)
    # similar_mangas_sum = similar_mangas.sum(skipna=True)
    # num_columns = similar_mangas.shape[1]
    # column_means = similar_mangas_sum / num_columns
    # sort = column_means.sort_values(ascending=False)
    # series = pd.Series(sort)
    # filtered_manga_ids = [manga_id for manga_id, score in series.items() if score >= 0]

    # Chính
    # similar_mangas = pd.concat(similar_mangas_list, axis=1).sum(axis=1).reset_index()
    # similar_mangas.columns = ['mangaId', 'score']
    # similar_mangas_cleaned = similar_mangas.dropna()
    # similar_mangas_cleaned
    # sort = similar_mangas_cleaned.sort_values(by='score',ascending=False)
    # df = pd.DataFrame(sort)
    # filtered_df = df[df['score'] >= 0]
    # filtered_manga_ids = filtered_df['mangaId'].tolist()

    # similar_mangas.head(10)
    similar_mangas_list = []
    similar_mangas_list.append(get_similar(rating.mangaId, rating.rate))
    similar_mangas = pd.concat(similar_mangas_list, axis=1).sum(axis=1).reset_index()
    similar_mangas.columns = ['mangaId', 'score']
    sort = similar_mangas.sort_values(by='score',ascending=False)
    similar_mangas_cleaned = sort.dropna()
    manga_id_list = list(map(ObjectId, similar_mangas_cleaned['mangaId'].tolist()))
    db.users.find_one_and_update(
        {'_id': ObjectId(rating.userId)},
        {'$set': {'recommendList': manga_id_list}}
    )
    return "Done"

    # db.users.find_one_and_update(
    #     {'_id': ObjectId(rating.userId)},
    #     {'$set': {'recommendList': filtered_manga_ids}}
    # )    

    # csv_path = './Collaborative Filtering/Recommendation_Rating.csv'
    
    # if os.path.exists(csv_path):
    #     # Đọc tệp CSV
    #     recommendation_df = pd.read_csv(csv_path)
        
    #     if rating.userId in recommendation_df.columns:
    #         recommendation_df[rating.userId] = similar_mangas_cleaned['mangaId']
    #     else:
    #         recommendation_df[rating.userId] = similar_mangas_cleaned['mangaId']
    # else:
    #     recommendation_df = pd.DataFrame({
    #         rating.userId: similar_mangas_cleaned['mangaId']
    #     })
    
    # recommendation_df.to_csv(csv_path, index=False)

def getRecommend(recommend):
    # filter = ['view','sumTimeRead','readingFrequency','numOfComment','rating','isFollow']
    filter = ['rating','isFollow']
    length = len(filter)
    random_num = random.randint(1, length)
    filter_chose = random.sample(filter, random_num)
    result_dict = {}
    result_dict["new"] = filter_new()
    for chosen_filter in filter_chose:
        if chosen_filter == 'rating':
            result_dict[chosen_filter] = filter_func_rating(recommend.userId)
            if len(filter_func_rating(recommend.userId)) == 0:
                result_dict[chosen_filter] = filter_func(recommend.userId,'isFollow')
        else:
            result_dict[chosen_filter] = filter_func(recommend.userId,chosen_filter)

    unique_values_set = set()
    for values in result_dict.values():
        unique_values_set.update(values)
    unique_values_list = sorted(unique_values_set)
    return unique_values_list

def filter_new():
    mangas_df = export_mangas_to_dict()
    mangas_df = mangas_df.sort_values(by="updatedAt", ascending=False)
    selected_ids = mangas_df['_id'].head(40).tolist()
    np.random.shuffle(selected_ids)
    selected_ids = selected_ids[:36]
    return selected_ids

def filter_func(id,fileName):
    # mangas_df = pd.read_csv('./Collaborative Filtering/mangas.csv')
    mangas_df = export_mangas_to_dict()
    # behaviors_df = pd.read_csv(f'./Collaborative Filtering/behaviors_{fileName}.csv')
    behaviors_df = export_data_to_dict(fileName)


    if fileName == "isFollow":
        behaviors_df['isFollow'] = behaviors_df['isFollow'].replace({True: 5, False: 0}).infer_objects(copy=False)

    # behaviors_df[fileName].fillna(0, inplace=True)
    behaviors_df[fileName] = behaviors_df[fileName].fillna(0)
    user_id_list = behaviors_df['userId'].unique()
    manga_id_list = mangas_df['_id'].unique()

    expanded_data = [{'userId': user_id, '_id': manga_id} for user_id in user_id_list for manga_id in manga_id_list]
    expanded_df = pd.DataFrame(expanded_data)
    expanded_df.rename(columns={'_id': 'mangaId'}, inplace=True)

    merged_df = pd.merge(expanded_df, behaviors_df, on=['userId', 'mangaId'], how='left')
    merged_df[fileName] = merged_df[fileName].fillna(0)
    merged_df.drop(columns=['updatedAt'], inplace=True)

    mangas_df.rename(columns={'_id': 'mangaId'}, inplace=True)
    datas = pd.merge(mangas_df, merged_df, on='mangaId').drop(['author','genre'],axis=1)

    userDatas = datas.pivot_table(index=['userId'],columns=['mangaId'],values=fileName)

    def standardize(row):
        return (row - row.mean()) / (row.max() - row.min())
    df_std = userDatas.apply(standardize).T.fillna(0)

    sparse_df = sparse.csr_matrix(df_std.values)
    cosine_sim_matrix = pd.DataFrame(cosine_similarity(sparse_df), index=userDatas.columns, columns=userDatas.columns)

    corrMatrix = userDatas.corr(method='pearson')

    # num_recommendations=36
    def get_recommendations(user_id, userViews, corrMatrix, num_recommendations=12):
        if user_id not in userViews.index:
                # If user_id does not exist, select a random user_id from userViews
                random_user_id = np.random.choice(userViews.index)
                user_data = userViews.loc[random_user_id]
        else:
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
    
    # recommendations = get_recommendations(id, userDatas, corrMatrix)
    recommendations = get_recommendations(id, userDatas, cosine_sim_matrix)

    # filtered_recommendations = [item_id for item_id, score in recommendations if not math.isnan(score)]
    filtered_recommendations = [item_id for item_id, score in recommendations if not np.isnan(score)]
    return filtered_recommendations

def filter_func_rating(id):
    # csv_path = './Collaborative Filtering/Recommendation_Rating.csv'
    # if os.path.exists(csv_path):
    #     recommendation_df = pd.read_csv(csv_path)
    #     if id in recommendation_df.columns:
    #         manga_id_list = recommendation_df[id].tolist()
    #     else:
    #         random_column = random.choice(recommendation_df.columns[0:])
    #         manga_id_list = recommendation_df[random_column].tolist()
    # else:
    #     manga_id_list = []
    manga_id_list = db.users.find_one({'_id': ObjectId(id)})['recommendList']
    recommend_list = list(map(str, manga_id_list))
    # print(recommend_list)
    # In ra danh sách mangaId
    return recommend_list[:12]

def getRecommentByGenre(mangaId):
    mangas_df = export_mangas_to_dict()
    tfidf = TfidfVectorizer(token_pattern=r'[a-zA-Z0-9\-]+')
    tfidf_matrix = tfidf.fit_transform(mangas_df['genre'])
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)

    def get_recommendations_by_manga_ids(manga_ids, cosine_sim=cosine_sim, mangas_df=mangas_df):
        recommended_mangas = pd.DataFrame(columns=['name', 'author', 'genre'])

        for manga_id in manga_ids:
            idx = mangas_df.index[mangas_df['_id'] == manga_id].tolist()

            if idx:
                idx = idx[0]
                sim_scores = list(enumerate(cosine_sim[idx]))
                sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
                sim_scores = sim_scores[1:11]
                manga_indices = [i[0] for i in sim_scores]
                recommended_mangas = pd.concat([recommended_mangas, mangas_df.iloc[manga_indices]], ignore_index=True)

        return recommended_mangas
    
    result = get_recommendations_by_manga_ids(mangaId)
    result = result['_id'].tolist()
    
    return result