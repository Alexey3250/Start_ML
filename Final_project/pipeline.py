import pandas as pd
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import pickle




engine = create_engine(
    "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
    "postgres.lab.karpov.courses:6432/startml"
)

# Чтение данных таблицы user_data
query = "SELECT * FROM user_data"
user_data = pd.read_sql(query, engine)

# Чтение данных таблицы post_text_df
query = "SELECT * FROM post_text_df"
post_text_df = pd.read_sql(query, engine)

# Чтение ограниченного количества данных таблицы feed_data
query = "SELECT * FROM feed_data LIMIT 100000"
feed_data = pd.read_sql(query, engine)

# Переименование столбцов идентификаторов
user_data = user_data.rename(columns={'id': 'user_id'})
post_text_df = post_text_df.rename(columns={'id': 'post_id'})

# Объединение таблиц
data = feed_data.merge(user_data, on='user_id', how='left')
data = data.merge(post_text_df, on='post_id', how='left')


# Convert the timestamp format to a datetime object
data['timestamp'] = pd.to_datetime(data['timestamp'])

# Extract features from the timestamp
data['day_of_week'] = data['timestamp'].dt.dayofweek
data['hour_of_day'] = data['timestamp'].dt.hour

# Calculate the time since the last action for each user
data = data.sort_values(['user_id', 'timestamp'])
data['time_since_last_action'] = data.groupby('user_id')['timestamp'].diff().dt.total_seconds()
data['time_since_last_action'].fillna(0, inplace=True)

# Drop the timestamp column
data = data.drop('timestamp', axis=1)

# One-hot encoding for 'country', 'city', and 'topic'
data = pd.get_dummies(data, columns=['country', 'city', 'topic'], prefix=['country', 'city', 'topic'])

from sklearn.preprocessing import LabelEncoder

le_gender = LabelEncoder()
le_os = LabelEncoder()
le_source = LabelEncoder()
le_action = LabelEncoder()

# Label encoding for 'gender', 'os', and 'source'
data['gender'] = le_gender.fit_transform(data['gender'])
data['os'] = le_os.fit_transform(data['os'])
data['source'] = le_source.fit_transform(data['source'])
data['action'] = le_action.fit_transform(data['action'])


# Feature 1: Количество просмотров и лайков для каждого пользователя
user_views_likes = data.groupby('user_id')['action'].value_counts().unstack().fillna(0)
user_views_likes.columns = ['user_views', 'user_likes']
data = data.merge(user_views_likes, on='user_id', how='left')

# Feature 2: Количество просмотров и лайков для каждого поста
post_views_likes = data.groupby('post_id')['action'].value_counts().unstack().fillna(0)
post_views_likes.columns = ['post_views', 'post_likes']
data = data.merge(post_views_likes, on='post_id', how='left')


temp_df = data[['exp_group', 'topic_business', 'topic_covid', 'topic_entertainment', 'topic_movie', 'topic_politics', 'topic_sport', 'topic_tech', 'action']]
for col in ['topic_business', 'topic_covid', 'topic_entertainment', 'topic_movie', 'topic_politics', 'topic_sport', 'topic_tech']:
    temp_df[col] = temp_df[col] * temp_df['action']
grouped_data = temp_df.groupby('exp_group').sum().reset_index()
grouped_data.columns = ['exp_group'] + [f'{col}_exp_group_views' if i % 2 == 0 else f'{col}_exp_group_likes' for i, col in enumerate(grouped_data.columns[1:], 1)]
data = data.merge(grouped_data, on='exp_group', how='left')


### Text features

import numpy as np
import re
from string import punctuation

def word_count(X):
    return np.array([len(re.findall(r'\b\w+\b', text)) for text in X])

def sentence_count(X):
    return np.array([len(re.findall(r'[.!?]+', text)) for text in X])

def avg_word_length(X):
    return np.array([sum(len(word) for word in re.findall(r'\b\w+\b', text)) / len(re.findall(r'\b\w+\b', text)) if len(re.findall(r'\b\w+\b', text)) > 0 else 0 for text in X])

def punctuation_count(X):
    return np.array([sum(1 for char in text if char in punctuation) for text in X])



# Apply the feature extraction functions to the 'text' column
word_counts = word_count(data['text'])
sentence_counts = sentence_count(data['text'])
avg_word_lengths = avg_word_length(data['text'])
punctuation_counts = punctuation_count(data['text'])

# Add the new features as columns in the DataFrame
data['word_count'] = word_counts
data['sentence_count'] = sentence_counts
data['avg_word_length'] = avg_word_lengths
data['punctuation_count'] = punctuation_counts


import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

# Load your data
# data = pd.read_csv('your_data.csv')

# Instantiate the TfidfVectorizer
vectorizer = TfidfVectorizer(max_features=1000)  # You can adjust max_features based on your needs

# Fit the vectorizer on the 'text' column and transform the text data
tfidf_matrix = vectorizer.fit_transform(data['text'])

# Convert the TF-IDF matrix to a DataFrame
tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=vectorizer.get_feature_names_out())

# Concatenate the original data with the TF-IDF DataFrame
data_with_tfidf = pd.concat([data.drop(columns=['text']), tfidf_df], axis=1)


import pandas as pd
import numpy as np
from sklearn.feature_selection import mutual_info_classif, SelectKBest

# Assume `data` is your DataFrame with features and target
X = data_with_tfidf.drop(['target', 'action'], axis=1)
y = data['target']

# Calculate mutual information between each feature and the target variable
mi_scores = mutual_info_classif(X, y, random_state=42)

# Create a DataFrame with feature names and their corresponding MI scores
mi_scores_df = pd.DataFrame({'feature': X.columns, 'mi_score': mi_scores})

# Sort the DataFrame by MI scores in descending order
mi_scores_df = mi_scores_df.sort_values('mi_score', ascending=False)

# Optionally, select the top k features using SelectKBest
k = 50
selector = SelectKBest(mutual_info_classif, k=k)
selector.fit(X, y)
selected_features = X.columns[selector.get_support()]

print("Top k features based on mutual information:")
print(selected_features)

# split the data into train and test
from sklearn.model_selection import train_test_split

# data with selected features, top k with mutual information, without data leakage, timestamp, 'action' and 'text'
X = data_with_tfidf[selected_features]
y = data_with_tfidf['target']

# Splitting the data into train and test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


# Sort the train and test sets by 'group_id'
X_train = X_train.sort_values(by='group_id')
y_train = y_train.loc[X_train.index]

X_test = X_test.sort_values(by='group_id')
y_test = y_test.loc[X_test.index]

# Create train and test Pool objects with the 'group_id' column
from catboost import Pool

train_pool = Pool(X_train.drop(columns=['user_id']), y_train, group_id=X_train['group_id'])
test_pool = Pool(X_test.drop(columns=['user_id']), y_test, group_id=X_test['group_id'])

# Train the CatBoost model using PrecisionAt:top=5 evaluation metric
from catboost import CatBoostClassifier

model = CatBoostClassifier(iterations=1000,
                           learning_rate=0.1,
                           depth=6,
                           custom_metric='PrecisionAt:top=5',
                           eval_metric='PrecisionAt:top=5',
                           random_seed=42,
                           verbose=100)

model.fit(train_pool, eval_set=test_pool)


model.save_model('catboost_model.cbm')

from_file = CatBoostClassifier()
from_file.load_model('catboost_model.cbm')
from_file.predict(test_pool)