import pandas as pd
from sqlalchemy import create_engine
import time




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
    


    chunksize = 100000
    
    # Чтение данных таблицы user_data
    query = "SELECT * FROM user_data"
    user_data = pd.read_sql(query, engine)
    
    # Чтение ограниченного данных таблицы feed_data
    query = "SELECT * FROM feed_data"
    feed_data = batch_load_sql_timed(engine, query, chunksize)
    
    # Переименование столбцов идентификаторов
    user_data = user_data.rename(columns={'id': 'user_id'})

    # Объединение таблиц
    data = feed_data.merge(user_data, on='user_id', how='left')
    
    return data

def main():
        
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    data = load_features()
    
    return data

if __name__ == "__main__":
    main()
