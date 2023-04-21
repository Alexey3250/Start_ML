import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
import catboost
from catboost import Pool, CatBoostClassifier, CatBoost
import numpy as np
import re
from string import punctuation
import os

'''
ФУНКЦИИ ПО ЗАГРУЗКЕ МОДЕЛЕЙ
'''
# Проверка если код выполняется в лмс, или локально
def get_model_path(path: str) -> str:
    """Просьба не менять этот код"""
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально. Немного магии
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

# Загрузка модели
def load_models(model_path):
    model = CatBoost()
    model.load_model(model_path)
    return model


'''
ФУНКЦИИ ПО ПОДГОТОВКЕ ДАННЫХ
'''
# Загрузка данных из базы данных
def load_and_merge_data(engine, feed_data_size):
    # Чтение данных таблицы user_data
    query = "SELECT * FROM user_data"
    user_data = pd.read_sql(query, engine)

    # Чтение данных таблицы post_text_df
    query = "SELECT * FROM post_text_df"
    post_text_df = pd.read_sql(query, engine)

    # Чтение ограниченного количества данных таблицы feed_data
    query = f"SELECT * FROM feed_data LIMIT {feed_data_size}"
    feed_data = pd.read_sql(query, engine)

    # Переименование столбцов идентификаторов
    user_data = user_data.rename(columns={'id': 'user_id'})
    post_text_df = post_text_df.rename(columns={'id': 'post_id'})

    # Объединение таблиц
    data = feed_data.merge(user_data, on='user_id', how='left')
    data = data.merge(post_text_df, on='post_id', how='left')
    
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
    return data

# Подготовка данных для инференса
def prepare_data_for_prediction(data):
    # Убираем ненужные столбцы
    X = data.drop(['target', 'action', 'text'], axis=1)

    categorical_columns = ['country', 'topic', 'city', 'gender', 'os', 'source']

    # Создание ID группы на основе столбца 'user_id'
    unique_user_ids = X['user_id'].unique()
    group_id_dict = {user_id: idx for idx, user_id in enumerate(unique_user_ids)}
    X['group_id'] = X['user_id'].map(group_id_dict)

    # Сортировка набора данных для предсказаний по 'group_id'
    X = X.sort_values(by='group_id')

    # Убедитесь, что категориальные переменные представлены в виде строк
    X[categorical_columns] = X[categorical_columns].astype(str)

    # Получение индексов категориальных столбцов
    cat_features = [X.drop(columns=['user_id']).columns.get_loc(col) for col in categorical_columns]

    # Создание объекта Pool для набора данных предсказаний с колонкой 'group_id' и категориальными признаками
    prediction_pool = Pool(X.drop(columns=['user_id']), cat_features=cat_features, group_id=X['group_id'])

    return prediction_pool


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

data = pd.DataFrame()

def main():
    # Загрузка обученной модели
    model_path = "models/catboost_MAP_model.cbm"
    model = load_models(model_path)
    print("Model loaded successfully.")
    
    # Загружаем данные
    inference_data = load_and_merge_data(engine, feed_data_size)
    print("Data loaded and merged successfully.")
    
    # Обработка данных
    inference_data = process_timestamps(inference_data)
    inference_data = create_additional_features(inference_data)
    print("Data processed successfully.")
    print(f"Data shape after process_inference_data: {inference_data.shape}")
    
    # Shape data for prediction
    prediction_pool = prepare_data_for_prediction(inference_data)
    print("Data shaped for prediction successfully.")
    
    # Предсказание
    predictions = model.predict(prediction_pool)
    
    
    """СОХРАНЕНИЕ РЕЗУЛЬТАТОВ"""
    # Сохранение результатов предсказаний в виде DataFrame
    predictions_df = pd.DataFrame(predictions, columns=['target'])
    predictions_df['user_id'] = inference_data['user_id'].reset_index(drop=True)
    predictions_df['post_id'] = inference_data['post_id'].reset_index(drop=True)

    # Группировка по user_id и сортировка по 'target' в порядке убывания
    grouped = predictions_df.groupby('user_id')
    top_5_predictions = grouped.apply(lambda x: x.nlargest(5, 'target')['post_id']).reset_index()

    # Группировка данных
    top_5_predictions['rank'] = top_5_predictions.groupby('user_id').cumcount() + 1

    # Преобразование данных в широкий формат
    top_5_predictions_wide = top_5_predictions.pivot_table(index='user_id', columns='rank', values='post_id')
    top_5_predictions_wide.columns = [f"Top_Prediction_{i}" for i in range(1, 6)]

    # Сброс индекса и сохранение предсказаний в CSV-файл
    top_5_predictions_wide.reset_index(inplace=True)
    top_5_predictions_wide.to_csv("top_5_predictions_wide.csv", index=False)

    print("Top-5 predictions saved to 'top_5_predictions_wide.csv'.")

if __name__ == "__main__":
    main()
