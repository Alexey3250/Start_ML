import pandas as pd
from sqlalchemy import create_engine
import time

import math
from tqdm import tqdm


def upload_dataframe_in_chunks(data, table_name, engine, chunksize=10000):
    total_chunks = math.ceil(len(data) / chunksize)
    for i in tqdm(range(total_chunks), desc=f"Uploading to {table_name}"):
        chunk = data[i * chunksize : (i + 1) * chunksize]
        if_exists = "replace" if i == 0 else "append"
        chunk.to_sql(table_name, con=engine, if_exists=if_exists, index=False, method="multi")

def load_features():
    
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
        )
    
    def batch_load_sql_timed(engine, query: str, chunksize: int) -> pd.DataFrame:
        conn = engine.connect().execution_options(stream_results=True)
        chunks = []
        row_count = 0
        start_time = time.time()

        for chunk_dataframe in pd.read_sql(query, conn, chunksize=chunksize):
            chunks.append(chunk_dataframe)
            row_count += len(chunk_dataframe)
            print(f"Loaded {row_count} rows, elapsed time: {time.time() - start_time:.2f} seconds")

        conn.close()
        return pd.concat(chunks, ignore_index=True)
    


    chunksize = 1000000
    
    # Чтение данных таблицы user_data
    query = "SELECT * FROM user_data"
    user_data = pd.read_sql(query, engine)
    print(f"User data shape: {user_data.shape}")
    
    # Чтение данных таблицы post_text_df
    query = "SELECT * FROM post_text_df"
    post_text_df = pd.read_sql(query, engine)
    print(f"Post text data shape: {post_text_df.shape}")
    
    # Чтение ограниченного данных таблицы feed_data
    query = "SELECT * FROM feed_data"
    feed_data = batch_load_sql_timed(engine, query, chunksize)
    print(f"Feed data shape: {feed_data.shape}")
    
    # Переименование столбцов идентификаторов
    user_data = user_data.rename(columns={'id': 'user_id'})
    post_text_df = post_text_df.rename(columns={'id': 'post_id'})

    # Объединение таблиц
    data = feed_data.merge(user_data, on='user_id', how='left')
    data = data.merge(post_text_df, on='post_id', how='left')

    print(f"Data shape after load_and_merge_data: {data.shape}")
    
    # Преобразование формата временных меток в объект datetime
    data['timestamp'] = pd.to_datetime(data['timestamp'])

    # Извлечение признаков из временных меток
    data['day_of_week'] = data['timestamp'].dt.dayofweek
    data['hour_of_day'] = data['timestamp'].dt.hour

    # Расчет времени с момента последнего действия для каждого пользователя
    data = data.sort_values(['user_id', 'timestamp'])
    data['time_since_last_action'] = data.groupby('user_id')['timestamp'].diff().dt.total_seconds()
    data['time_since_last_action'].fillna(0, inplace=True)

    # Extracting day of the month and year from the timestamp
    data['day_of_month'] = data['timestamp'].dt.day
    data['year'] = data['timestamp'].dt.year

    # Удаление столбца временных меток
    data = data.drop('timestamp', axis=1)

    print('Timestamps processed')
    print(f"Data shape after timestamps processing: {data.shape}")

    # Feature 1: Количество просмотров и лайков для каждого пользователя
    user_views_likes = data.groupby('user_id')['action'].value_counts().unstack().fillna(0)
    user_views_likes.columns = ['user_views', 'user_likes']
    data = data.merge(user_views_likes, on='user_id', how='left')

    # Feature 2: Количество просмотров и лайков для каждого поста
    post_views_likes = data.groupby('post_id')['action'].value_counts().unstack().fillna(0)
    post_views_likes.columns = ['post_views', 'post_likes']
    data = data.merge(post_views_likes, on='post_id', how='left')

    # Feature 3: Количество просмотров и лайков для каждой группы тематик
    temp_df = data[['exp_group', 'topic', 'action']]

    # Создание колонок с количеством просмотров и лайков для каждой темы внутри группы
    topic_action_count = temp_df.pivot_table(index='exp_group', columns=['topic', 'action'], aggfunc=len, fill_value=0)
    topic_action_count.columns = [f'{col[0]}_exp_group_{col[1]}s' for col in topic_action_count.columns]
    grouped_data = topic_action_count.reset_index()

    data = data.merge(grouped_data, on='exp_group', how='left')

    # Преобразование категориальных признаков в строковый формат
    categorical_columns = ['country', 'city', 'topic', 'gender', 'os', 'source']
    data[categorical_columns] = data[categorical_columns].astype(str)

    print('Additional features created')
    print(f"Data shape after additional features creation: {data.shape}")
    
    return data

def main():
        
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    data = load_features()
    
    chunksize = 10000
    upload_dataframe_in_chunks(data, "a-efimik_features_lesson_22", engine, chunksize=chunksize)

if __name__ == "__main__":
    main()
