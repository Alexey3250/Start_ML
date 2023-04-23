'''
Работаем по кусочкам, потому что данные очень большие
    Взять данные из базы данных
    обработать данные
    залить данные в базу данных в новой таблице
'''

import pandas as pd
from sqlalchemy import create_engine

'''
ФУНКЦИИ ПО ЗАГРУЗКЕ ДАННЫХ
'''
# Загрузка данных из базы данных
def load_and_merge_data(engine, feed_data_size, batch_size):
    # Чтение данных таблицы user_data
    query = "SELECT * FROM user_data"
    user_data = pd.read_sql(query, engine)

    # Чтение данных таблицы post_text_df
    query = "SELECT * FROM post_text_df"
    post_text_df = pd.read_sql(query, engine)

    # Создание переменной для итерации по батчам данных
    data = pd.DataFrame()

    # Чтение ограниченного количества данных таблицы feed_data с использованием yield_per
    query = f"SELECT * FROM feed_data LIMIT {feed_data_size}"
    with engine.connect() as conn:
        for feed_data_batch in yield_per(conn.execution_options(stream_results=True).execute(query), batch_size):
            # Преобразование батча результатов в DataFrame
            feed_data = pd.DataFrame(feed_data_batch)

            # Переименование столбцов идентификаторов
            user_data = user_data.rename(columns={'id': 'user_id'})
            post_text_df = post_text_df.rename(columns={'id': 'post_id'})

            # Объединение таблиц
            data_batch = feed_data.merge(user_data, on='user_id', how='left')
            data_batch = data_batch.merge(post_text_df, on='post_id', how='left')

            # Добавление текущего батча данных к общему набору данных
            data = pd.concat([data, data_batch])

        print(f"Data shape after load_and_merge_data: {data.shape}")

    return data

# Обработка временных меток
def process_timestamps(data):
    # Преобразование формата временных меток в объект datetime
    data['timestamp'] = pd.to_datetime(data['timestamp'])

    # Извлечение признаков из временных меток
    data['day_of_week'] = data['timestamp'].dt.dayofweek
    data['hour_of_day'] = data['timestamp'].dt.hour

    # Расчет времени с момента последнего действия для каждого пользователя
    data = data.sort_values(['user_id', 'timestamp'])
    data['time_since_last_action'] = data.groupby('user_id')['timestamp'].diff().dt.total_seconds()
    data['time_since_last_action'].fillna(0, inplace=True)

    # Удаление столбца временных меток
    data = data.drop('timestamp', axis=1)
    
    print('Timestamps processed')
    
    return data

# Создание дополнительных признаков
def create_additional_features(data):
    # Feature 1: Количество просмотров и лайков для каждого пользователя
    user_views_likes = data.groupby('user_id')['action'].value_counts.unstack().fillna(0).reset_index()
    user_views_likes.columns = ['user_id', 'num_likes', 'num_views']
    data = data.merge(user_views_likes, on='user_id', how='left')

    # Feature 2: Среднее количество слов в постах, просмотренных пользователем
    data['num_words'] = data['text'].str.split().str.len()
    avg_words_per_viewed_post = data[data['action'] == 'view'].groupby('user_id')['num_words'].mean().reset_index()
    avg_words_per_viewed_post.columns = ['user_id', 'avg_words_per_viewed_post']
    data = data.merge(avg_words_per_viewed_post, on='user_id', how='left')

    # Feature 3: Доля лайков относительно общего количества действий для каждого пользователя
    data['like_ratio'] = data['num_likes'] / (data['num_likes'] + data['num_views'])

    print('Additional features created')

    return data

'''
ВАЖНЫЕ ПЕРЕМЕННЫЕ
'''
# Для работы с БД
engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )

# Сколько рядов данных загружать за один раз
feed_data_size = 100000

def main():
    # Загружаем данные
    batch_size = 1000  # задайте значение batch_size
    data = load_and_merge_data(engine, feed_data_size, batch_size)
    print("Data loaded and merged successfully.")
    
    # Обработка временных меток
    data = process_timestamps(data)

    # Создание дополнительных признаков
    data = create_additional_features(data)
    
    # Здесь можно добавить код обработки данных, если это необходимо

    # Сохранение данных в CSV-файл
    data.to_csv("data.csv", index=False)
    print("Data saved to 'data.csv'.")

if __name__ == "__main__":
    main()
