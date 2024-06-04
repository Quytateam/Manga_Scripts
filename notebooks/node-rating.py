import pandas as pd

fileName = "rating"
mangas_df = pd.read_csv('../Collaborative Filtering/mangas.csv')
behaviors_df = pd.read_csv(f'../Collaborative Filtering/behaviors_{fileName}.csv')

behaviors_df[fileName].fillna(0, inplace=True)

user_id_list = behaviors_df['userId'].unique()
manga_id_list = mangas_df['_id'].unique()
expanded_data = []

for user_id in user_id_list:
    for manga_id in manga_id_list:
        expanded_data.append({'userId': user_id, '_id': manga_id})

expanded_df = pd.DataFrame(expanded_data)

expanded_df.rename(columns={'_id': 'mangaId'}, inplace=True)
merged_df = pd.merge(expanded_df, behaviors_df, on=['userId', 'mangaId'], how='left')

merged_df[fileName] = merged_df[fileName].fillna(0)
merged_df.drop(columns=['updatedAt'], inplace=True)

mangas_df.rename(columns={'_id': 'mangaId'}, inplace=True)
datas = pd.merge(mangas_df, merged_df, on='mangaId').drop(['author','genre'],axis=1)

userDatas = datas.pivot_table(index=['userId'],columns=['mangaId'],values=fileName)
print("Before: ",userDatas.shape)
userDatas = userDatas.dropna(thresh=10, axis=1).fillna(0,axis=1)
#userRatings.fillna(0, inplace=True)
print("After: ",userDatas.shape)

corrMatrix = userDatas.corr(method='pearson')

def get_similar(manga_Id,data):
    similar_datas = corrMatrix[manga_Id]*(data-2.5)
    similar_datas = similar_datas.sort_values(ascending=False)
    #print(type(similar_ratings))
    return similar_datas

test = [("662a9e8bd2a9173d53c01bb5",5),("662a9e86d2a9173d53c01bb0",3),("662a9e8bd2a9173d53c01bb5",1),("662a9e7ed2a9173d53c01ba8",2)]
similar_mangas_list = []
for manga, data in test:
    similar_mangas_list.append(get_similar(manga, data))
similar_mangas = pd.concat(similar_mangas_list, axis=1).sum(axis=1).reset_index()
similar_mangas.columns = ['mangaId', 'score']
similar_mangas_cleaned = similar_mangas.dropna()
similar_mangas_cleaned