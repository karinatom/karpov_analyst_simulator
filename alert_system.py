# устанавливаем нужные библиотеки
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io 
import pandas as pd
import pandahouse as ph
import sys 
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# получаем доступ к нашему боту
my_token = # прописываем токен бота
bot = telegram.Bot(token=my_token) # получаем к нему доступ

chat_id = 239606887
#-644906564

# подключаемся к базе данных
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220920',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'k-tomnikovskaja-11',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 22)
}

# повторяем цикл каждые 15 минут
schedule_interval = '*/15 * * * *'

# списки с набором интересующих нас метрик
metrics_feed = ['users_feed', 'views', 'likes', 'ctr']
metrics_message = ['users_messager', 'messages']

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_k_tomnikovskaja_11_8():
    
    @task
    # выгружаем данные из  ленты
    def extract_feed():
        query_feed = """SELECT toStartOfFifteenMinutes(time) AS ts,
                                   toDate(time) AS date,
                                   formatDateTime(ts, '%R') AS hm,
                                   uniqExact(user_id) AS users_feed,
                                   countIf(user_id, action='view') AS views,
                                   countIf(user_id, action='like') AS likes,
                                   countIf(user_id, action='like') / countIf(user_id, action='view') AS ctr
                            FROM simulator_20220920.feed_actions
                            WHERE time >= yesterday() AND time < toStartOfFifteenMinutes(now())
                            GROUP BY ts, date, hm
                            ORDER BY ts""" # округляем время до текущей пятнадцатиминутки и не учитываем эту неполную пятнадцатиминутку

        feed = ph.read_clickhouse(query_feed, connection=connection)
        
        return feed
    
    @task
    # выгружаем данные из мессенджера
    def extract_messages():
        query_messages = """SELECT toStartOfFifteenMinutes(time) AS ts,
                                    toDate(time) AS date,
                                    formatDateTime(ts, '%R') AS hm,
                                    uniqExact(user_id) AS users_messager,
                                    COUNT(reciever_id) AS messages
                            FROM simulator_20220920.message_actions
                            WHERE time >= yesterday() AND time < toStartOfFifteenMinutes(now())
                            GROUP BY ts, date, hm
                            ORDER BY ts"""

        messages = ph.read_clickhouse(query_messages, connection=connection)
        
        return messages 
    
    @task
    # функция запускает сообщение с алертом    
    def alerts(data, metrics):

        # функция использует стандартное отклонение для поиска аномалий в данных
        def check_anomaly(df, metric, a=3, n=6):

            df['mean'] = df[metric].shift(1).rolling(n).mean()
            df['sd'] = df[metric].shift(1).rolling(n).std()

            df['top'] = df['mean'] + a * df['sd']
            df['bottom'] = df['mean'] - a * df['sd']

            # сгладим границы
            df['top'] = df['top'].rolling(n, center=True, min_periods=1).mean()
            df['bottom'] = df['bottom'].rolling(n, center=True, min_periods=1).mean()

            if df[metric].iloc[-1] < df['bottom'].iloc[-1] or df[metric].iloc[-1] > df['top'].iloc[-1]:
                is_alert = 1
            else:
                is_alert = 0

            return is_alert

        for metric in metrics:
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert = check_anomaly(df, metric)

            if is_alert == 1:

                current_val = df[metric].iloc[-1] 
                last_val_diff = (df[metric].iloc[-1] - float(df[metric].iloc[-2])) / float(df[metric].iloc[-2]) * 100
                yesterday_diff = (df[metric].iloc[-1] - float(df[df.ts == df.iloc[-1].ts - timedelta(days=1)][metric])) / float(df[df.ts == df.iloc[-1].ts - timedelta(days=1)][metric]) * 100
                current_time = df.iloc[-1]['hm']

                msg = f'⚠️ALERT⚠️\n\nМетрика {metric} в {current_time}\nТекущее значение: {current_val:.2f}\nОтклонение от предыдущего значения: {round(last_val_diff, 2)}%,\nОтклонение от показателей за вчера в тоже время: {round(yesterday_diff, 2)}%\nДашборд: https://superset.lab.karpov.courses/superset/dashboard/1960/'


                sns.set(rc={'figure.figsize': (16,10)})
                plt.tight_layout()

                ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
                ax = sns.lineplot(x=df['ts'], y=df['top'], label='top')
                ax = sns.lineplot(x=df['ts'], y=df['bottom'], label='bottom')

                ax.set(xlabel='time')
                ax.set(ylabel=metric)
                ax.set_title(metric)
                # укажем нижнюю границу равную нулю
                ax.set(ylim=(0,None))
                plt.fill_between(x=df['ts'], y1=df['top'], y2=df['bottom'], alpha=0.3)

                plot_object = io.BytesIO() # создаем файловый объект
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = 'alert.png'.format(metric)
                plt.close()

                bot.sendMessage(chat_id=chat_id, text=msg)  
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                
    feed = extract_feed()
    messages = extract_messages()
    alerts(feed, metrics_feed)
    alerts(messages, metrics_message)
    
dag_k_tomnikovskaja_11_8 = dag_k_tomnikovskaja_11_8()
