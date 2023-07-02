from catboost import CatBoostClassifier
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
def load_models():
    model_path = "C:/Users/Alex/Desktop/Repos/Start_ML/Deep_learning/Final_2/catboost_model.cbm"
    model = CatBoostClassifier()
    model.load_model(model_path, format="cbm")
    print("Model loaded successfully.")
    return model

load_models()