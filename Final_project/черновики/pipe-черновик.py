import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
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

# Кодирование категориальных признаков
def encode_categorical_features(data):
    # One-hot encoding для 'country', 'city' и 'topic'
    data = pd.get_dummies(data, columns=['country', 'city', 'topic'], prefix=['country', 'city', 'topic'])

    le_gender = LabelEncoder()
    le_os = LabelEncoder()
    le_source = LabelEncoder()
    le_action = LabelEncoder()

    # Label encoding для 'gender', 'os' и 'source'
    data['gender'] = le_gender.fit_transform(data['gender'])
    data['os'] = le_os.fit_transform(data['os'])
    data['source'] = le_source.fit_transform(data['source'])
    data['action'] = le_action.fit_transform(data['action'])

    print('Categorical features encoded')
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
    temp_df = data[['exp_group', 'topic_business', 'topic_covid', 'topic_entertainment', 'topic_movie', 'topic_politics', 'topic_sport', 'topic_tech', 'action']]
    for col in ['topic_business', 'topic_covid', 'topic_entertainment', 'topic_movie', 'topic_politics', 'topic_sport', 'topic_tech']:
        temp_df.loc[:, col] = temp_df[col] * temp_df['action']
    grouped_data = temp_df.groupby('exp_group').sum().reset_index()
    grouped_data.columns = ['exp_group'] + [f'{col}_exp_group_views' if i % 2 == 0 else f'{col}_exp_group_likes' for i, col in enumerate(grouped_data.columns[1:], 1)]
    data = data.merge(grouped_data, on='exp_group', how='left')

    print('Additional features created')
    return data

# Обработка текстовых признаков
def process_text_features(data):
    def word_count(X):
        return np.array([len(re.findall(r'\b\w+\b', text)) for text in X])
    def sentence_count(X):
        return np.array([len(re.findall(r'[.!?]+', text)) for text in X])
    def avg_word_length(X):
        return np.array([sum(len(word) for word in re.findall(r'\b\w+\b', text)) / len(re.findall(r'\b\w+\b', text)) if len(re.findall(r'\b\w+\b', text)) > 0 else 0 for text in X])
    def punctuation_count(X):
        return np.array([sum(1 for char in text if char in punctuation) for text in X])

    # Применение функций извлечения признаков к столбцу 'text'
    word_counts = word_count(data['text'])
    sentence_counts = sentence_count(data['text'])
    avg_word_lengths = avg_word_length(data['text'])
    punctuation_counts = punctuation_count(data['text'])

    # Добавление новых признаков в виде столбцов в DataFrame
    data['word_count'] = word_counts
    data['sentence_count'] = sentence_counts
    data['avg_word_length'] = avg_word_lengths
    data['punctuation_count'] = punctuation_counts

    print('Text features processed')
    return data

# Обработка TF-IDF
def process_tfidf(data):
    # Инициализация TfidfVectorizer
    vectorizer = TfidfVectorizer(max_features=1000) # Вы можете настроить max_features в зависимости от ваших потребностей
    
    # Обучение vectorizer на столбце 'text' и преобразование текстовых данных
    tfidf_matrix = vectorizer.fit_transform(data['text'])

    # Преобразование матрицы TF-IDF в DataFrame
    tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=vectorizer.get_feature_names_out())

    # Конкатенация исходных данных с DataFrame TF-IDF
    data_with_tfidf = pd.concat([data.drop(columns=['text']), tfidf_df], axis=1)

    print('TF-IDF processed')
    return data_with_tfidf


'''
ФУНККЦИИ ДЛЯ ОБУЧЕНИЯ МОДЕЛИ 
'''
# Отбор признаков на основе Mutual Information
''' 
# Отбор признаков на основе взаимной информации
def select_features_mi(data, target_col, k=50):
    X = data.drop([target_col], axis=1)
    y = data[target_col]
    
    # Вычисление взаимной информации между каждым признаком и целевой переменной
    mi_scores = mutual_info_classif(X, y, random_state=42)

    # Создание DataFrame с именами признаков и соответствующими им оценками MI
    mi_scores_df = pd.DataFrame({'feature': X.columns, 'mi_score': mi_scores})

    # Сортировка DataFrame по оценкам MI в порядке убывания
    mi_scores_df = mi_scores_df.sort_values('mi_score', ascending=False)

    # При необходимости выберите k лучших признаков с помощью SelectKBest
    selector = SelectKBest(mutual_info_classif, k=k)
    selector.fit(X, y)
    selected_features = X.columns[selector.get_support()]

    print("Top k features based on mutual information:")
    print(selected_features)

    return selected_features
'''

# Функция тренировки модели CatBoost
'''
# Обучение модели CatBoost
def train_catboost_model(X_train, X_test, y_train, y_test, group_id_col):
    # Сортируем наборы данных для обучения и тестирования по 'group_id'
    X_train = X_train.sort_values(by=group_id_col)
    y_train = y_train.loc[X_train.index]
    
    X_test = X_test.sort_values(by=group_id_col)
    y_test = y_test.loc[X_test.index]

    # Создание объектов Pool для обучения и тестирования с указанием столбца 'group_id'
    train_pool = Pool(X_train.drop(columns=[group_id_col]), y_train, group_id=X_train[group_id_col])
    test_pool = Pool(X_test.drop(columns=[group_id_col]), y_test, group_id=X_test[group_id_col])

    # Обучение модели CatBoost с использованием метрики оценки PrecisionAt:top=5
    model = CatBoostClassifier(iterations=1000,
                            learning_rate=0.1,
                            depth=6,
                            custom_metric='PrecisionAt:top=5',
                            eval_metric='PrecisionAt:top=5',
                            random_seed=42,
                            verbose=100)

    model.fit(train_pool, eval_set=test_pool)

    return model

# Сохранение и загрузка модели CatBoost
def save_and_load_catboost_model(model, model_path):
    model.save_model(model_path)
    loaded_model = CatBoostClassifier()
    loaded_model.load_model(model_path)

    return loaded_model
'''


'''
ОСНОВНАЯ ЧАСТЬ ПРОГРАММЫ
'''

# Создали функцию для обработки данных
def process_inference_data(data, preselected_features):
    '''
    На выходе мы получим обработанные данные, которые можно будет передать в модель для получения прогнозов.
    '''
    
    # Обработка данных
    data = process_timestamps(data)
    data = encode_categorical_features(data)
    data = create_additional_features(data)
    data = process_text_features(data)
    data = process_tfidf(data)
        
    # Добавление отсутствующих колонок и заполнение их нулями
    for col in preselected_features:
        if col not in data.columns:
            data[col] = 0

    # Оставить только предварительно отобранные признаки
    data = data[preselected_features]
    
    return data

# Функция для получения прогнозов
def catboost_inference(model, inference_data):
    """
    Функция выполняет предсказания для каждого пользователя в данных.

    Аргументы:
        model: обученная модель CatBoost.
        inference_data: DataFrame с данными для предсказания.

    Возвращает:
        predictions: массив с предсказаниями.
    """

    # Создаем столбец 'group_id' на основе столбца 'user_id'
    unique_user_ids = inference_data['user_id'].unique()
    group_id_dict = {user_id: idx for idx, user_id in enumerate(unique_user_ids)}
    inference_data['group_id'] = inference_data['user_id'].map(group_id_dict)

    # Удаляем столбцы 'user_id' и 'group_id' перед выполнением предсказаний
    inference_data_no_ids = inference_data.drop(columns=['user_id', 'group_id'])

    # Выполняем предсказания
    predictions = model.predict(inference_data_no_ids)

    return predictions

# Функция для сохранения прогнозов
def save_predictions(predictions, file_name):
    # Ensure the "predictions" subfolder exists
    if not os.path.exists("predictions"):
        os.makedirs("predictions")

    # Save predictions to CSV file in the "predictions" subfolder
    predictions_df = pd.DataFrame(predictions, columns=["prediction"])
    predictions_df.to_csv(f"predictions/{file_name}", index=False)

    print(f"Predictions saved to 'predictions/{file_name}'.")


'''
ВАЖНЫЕ ПЕРЕМЕННЫЕ
'''
# Для работы с БД
engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )

# Отобранные признаки для модели
preselected_features = ['user_id', 'gender', 'exp_group', 'os', 'day_of_week', 'hour_of_day',
    'time_since_last_action', 'country_Russia', 'city_Kansk', 'city_Kazan',
    'city_Mangghystaū', 'city_Michurinsk', 'city_Moscow', 'city_Pavlovo',
    'city_Samara', 'topic_covid', 'topic_entertainment', 'topic_movie',
    'user_views', 'user_likes', 'post_views',
    'topic_business_exp_group_likes', 'topic_covid_exp_group_views',
    'topic_entertainment_exp_group_likes', 'topic_movie_exp_group_views',
    'topic_politics_exp_group_likes', 'topic_sport_exp_group_views',
    'topic_tech_exp_group_likes', 'action_exp_group_views',
    'sentence_count', 'punctuation_count', 'australian', 'based', 'city',
    'ending', 'failed', 'given', 'idea', 'life', 'likely', 'opportunity',
    'order', 'paid', 'party', 'past', 'political', 'robert', 'scene',
    'seem', 'wanted']

# Сколько рядов данных загружать за один раз
feed_data_size = 1000

data = pd.DataFrame()

'''
ГЛАВНАЯ ФУНКЦИЯ
'''
# Загрузка и обработка данных для инференса -> Обработка и сохранение результатов предсказаний
def main():
    # Загрузка обученной модели
    model_path = "models/catboost_MAP_model.cbm"
    model = load_models(model_path)
    print("Model loaded successfully.")
    
    # Загружаем данные
    data = load_and_merge_data(engine, feed_data_size)
    print("Data loaded and merged successfully.")
    
    # Обработка данных
    data = process_inference_data(data, preselected_features)
    print("Data processed successfully.")
    print(f"Data shape after process_inference_data: {data.shape}")
    
    # Выполнение инференса
    predictions = catboost_inference(model, data)
    print("Predictions made successfully.")

    # Save predictions to a CSV file
    save_predictions(predictions, "predictions.csv")

if __name__ == "__main__":
    main()