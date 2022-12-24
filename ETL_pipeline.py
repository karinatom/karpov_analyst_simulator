from datetime import datetime, timedelta
import pandas as pd
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'k-tomnikovskaja-11',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 14)
}

# Интервал запуска DAG
schedule_interval = '0 13 * * *'

# подключаемся к базам данных
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220920',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

test_connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
                     }

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_k_tomnikovskaja_11():

# извлекаем данные из кликхауса
    @task
    def extract_feed():
        query_feed = """SELECT toDate(time) AS event_date,
                               user_id, age, gender, os,
                               countIf(action, action='like') AS likes,
                               countIf(action, action='view') AS views
                        FROM simulator_20220920.feed_actions 
                        WHERE event_date = yesterday()
                        GROUP BY user_id, event_date, age, gender, os"""

        feed = ph.read_clickhouse(query_feed, connection=connection)
        return feed

    @task
    def extract_messaages():
        query_message = """SELECT event_date,
                                  user_id,
                                  messages_received,
                                  messages_sent,
                                  users_received,
                                  users_sent, age, gender, os
                           FROM
                           (SELECT toDate(time) AS event_date,
                                   user_id,
                                   COUNT(reciever_id) AS messages_sent,
                                   COUNT(DISTINCT reciever_id) AS users_sent,
                                   age, gender, os
                           FROM simulator_20220920.message_actions
                           WHERE event_date = yesterday()
                           GROUP BY user_id, event_date, age, gender, os) l

                           JOIN

                           (SELECT toDate(time) AS event_date,
                                   reciever_id,
                                   COUNT(user_id) AS messages_received,
                                   COUNT(DISTINCT user_id) AS users_received
                           FROM simulator_20220920.message_actions
                           WHERE event_date = yesterday()
                           GROUP BY reciever_id, event_date) r

                           ON l.user_id = r.reciever_id"""

        message = ph.read_clickhouse(query_message, connection=connection)
        return message
    
    @task
    # объединение датафреймов из ленты и мессенджера
    def merge_df(feed, message):
        merged_data = feed.merge(message, how='outer', on=['user_id', 'event_date', 'age', 'gender', 'os'])

        return merged_data
    
    @task
    # срез по OS
    def os_dimention(merged_data):
        os = merged_data.groupby(['os','event_date'], as_index=False)['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum()
        os['dimension'] = 'os'
        os.rename(columns={'os':'dimension_value'}, inplace=True)

        return os
    
    @task
    # срез по полу
    def gender_dimention(merged_data):
        gender = merged_data.groupby(['gender','event_date'], as_index=False)['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum()
        gender['dimension'] = 'gender'
        gender.rename(columns={'gender':'dimension_value'}, inplace=True)

        return gender
    
    @task
    # срез по возрасту
    def age_dimention(merged_data):
        age = merged_data.groupby(['age','event_date'], as_index=False)['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum()
        age['dimension'] = 'age'
        age.rename(columns={'age':'dimension_value'}, inplace=True)

        return age
    
    @task
    # объединение срезов в один датафрейм
    def final_join(age, os, gender):
        final = pd.concat([age, os, gender])
        final = final.reindex(columns=['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'])
        final = final.astype({'event_date' : 'datetime64', 'views': 'int32', 'likes': 'int32', 'messages_received': 'int32', 'messages_sent': 'int32', 'users_received': 'int32', 'users_sent': 'int32'})

        return final
    
    @task
    # выгружаем итоговую таблицу
    def load(final):
        query = """CREATE TABLE IF NOT EXISTS test.k_tomnikovskaja_11
            (event_date Date, 
             dimension String,
             dimension_value String,
             views Int32,
             likes Int32,
             messages_received Int32,
             messages_sent Int32,
             users_received Int32,
             users_sent Int32) 
             ENGINE = MergeTree()
             ORDER BY event_date"""
        
        ph.execute(query=query, connection=test_connection)
        ph.to_clickhouse(df=final, table='k_tomnikovskaja_11', connection=test_connection, index=False)
    
    # последовательность выполнения функций
    feed = extract_feed()
    message = extract_messaages()
    merged_data = merge_df(feed, message)
    os = os_dimention(merged_data)
    gender = gender_dimention(merged_data)
    age = age_dimention(merged_data)
    final = final_join(age, os, gender)
    load(final)
    
dag_k_tomnikovskaja_11 = dag_k_tomnikovskaja_11()

