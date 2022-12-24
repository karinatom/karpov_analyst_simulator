# устанавливаем нужные библиотеки
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# получаем доступ к нашему боту
my_token = '5711016012:AAGFpwQFPOOV8b2FVsrCn3wmhwc1GIwotIM'
bot = telegram.Bot(token=my_token)

chat_id = -769752736

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
    'start_date': datetime(2022, 10, 17)
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_k_tomnikovskaja_11_7_3():

    @task
    # выгрузим DAU пользователей, которые используют и ленту и приложение 
    def extract_both():
        query_dau_both = """SELECT date, MIN(users) AS users
                            FROM
                              (SELECT date, uniq(user_id) AS users
                               FROM
                                 (SELECT user_id, toDate(time) AS date
                                  FROM simulator_20220920.feed_actions
                                  GROUP BY user_id, date

                                  UNION distinct 

                                  SELECT user_id, toDate(time) AS date
                                  FROM simulator_20220920.message_actions
                                  GROUP BY user_id, date)
                                GROUP BY date) AS t
                            WHERE date >= today() - 8 AND date != today()
                            GROUP BY date
                            ORDER BY date"""

        dau_both = ph.read_clickhouse(query_dau_both, connection=connection)
        return dau_both

    @task
    # выгружаем данные из  ленты
    # DAU и CTR
    def extract_feed():
        query_feed = """SELECT toDate(time) AS date,
                               count(DISTINCT user_id) AS DAU,
                               countIf(user_id, action='like') / countIf(user_id, action='view') AS CTR
                        FROM simulator_20220920.feed_actions
                        WHERE date >= today() - 8 AND date != today()
                        GROUP BY date
                        ORDER BY date"""

        feed = ph.read_clickhouse(query_feed, connection=connection)
        return feed

    @task
    # лайки и просмотры
    def extract_views():
        query_likes_views = """SELECT toDate(time) AS date,
                                      action,
                                      count(user_id) AS users
                               FROM simulator_20220920.feed_actions
                               WHERE date >= today() - 8 AND date != today()
                               GROUP BY action,
                                        date
                               ORDER BY date"""

        likes_views = ph.read_clickhouse(query_likes_views, connection=connection)
        return likes_views

    @task
    # retention за 7 дней
    def extract_retention():
        query_retention = """SELECT toStartOfDay(toDate(date)) AS date, source, SUM(users) AS total_users
                            FROM
                              (SELECT date, COUNT(user_id) AS users, source
                               FROM
                                 (SELECT user_id, source
                                  FROM simulator_20220920.feed_actions
                                  GROUP BY user_id, source
                                  HAVING min(toDate(time)) = today() - 7) l
                               JOIN
                                 (SELECT DISTINCT user_id, toDate(time) AS date, source
                                  FROM simulator_20220920.feed_actions) r 
                               USING user_id
                               GROUP BY date, source) AS r
                            WHERE date != today()
                            GROUP BY source, date
                            ORDER BY date """

        retention = ph.read_clickhouse(query_retention, connection=connection)
        return retention

    @task
    # выгружаем данные из мессенджера
    # информация о пользователях, которые используют только мессенджер
    def extract_messages():
        query_avg_messages =  """SELECT toDate(time) AS date, COUNT(reciever_id) AS messages,
                                        ROUND(COUNT(reciever_id) / COUNT(DISTINCT user_id), 2) AS avg_messages
                                FROM simulator_20220920.message_actions
                                WHERE date >= today() - 8 AND date != today()
                                GROUP BY date
                                ORDER BY date"""

        avg_messages = ph.read_clickhouse(query_avg_messages, connection=connection)
        return avg_messages

    @task
    def extract_messages_dau():
        query_dau_messages =  """SELECT toDate(time) AS date, COUNT(DISTINCT user_id) AS DAU
                                FROM simulator_20220920.feed_actions f
                                INNER JOIN simulator_20220920.message_actions m
                                ON f.user_id = m.user_id AND toDate(f.time) = toDate(m.time)
                                WHERE date >= today() - 8 AND date != today()
                                GROUP BY date
                                ORDER BY date"""

        dau_messages = ph.read_clickhouse(query_dau_messages, connection=connection)
        return dau_messages
    
# соберем сообщение с метриками
    @task
    def message(dau_both, likes_views, feed, avg_messages, dau_messages):
        # соберем сообщение
        # подсчитаем разницу между вчерашними показателями и показателями прошлой недели
        week_diff_dau = round((int(dau_both.iloc[-1].users) - int(dau_both.iloc[0].users)) / int(dau_both.iloc[0].users) * 100, 2)

        week_diff_dau_feed = round((int(feed.iloc[-1].DAU) - int(feed.iloc[0].DAU)) / int(feed.iloc[0].DAU) * 100, 2)
        week_diff_ctr = round((float(feed.iloc[-1].CTR) - float(feed.iloc[0].CTR)) / float(feed.iloc[0].CTR) * 100, 2)

        week_diff_likes = round((int(likes_views.iloc[-1].users) - int(likes_views.iloc[0].users)) / int(likes_views.iloc[0].users) * 100, 2)
        week_diff_views = round((int(likes_views.iloc[-2].users) - int(likes_views.iloc[1].users)) / int(likes_views.iloc[1].users) * 100, 2)

        week_diff_dau_messages = round((int(dau_messages.iloc[-1].DAU) - int(dau_messages.iloc[0].DAU)) / int(dau_messages.iloc[0].DAU) * 100, 2)
        week_diff_messages = round((int(avg_messages.iloc[-1].messages) - int(avg_messages.iloc[0].messages)) / int(avg_messages.iloc[0].messages) * 100, 2)
        date = str(feed.iloc[-1].date)[:10]
        text = f'Ключевые метрики за {date}:\n \
        ----------------------\n \
        DAU \n\n Общая аудитория: {dau_both.iloc[-1].users}, \n Неделю назад: {dau_both.iloc[0].users}|{week_diff_dau}% \n\n \
        Только лента: {feed.iloc[-1].DAU}, \n Неделю назад: {feed.iloc[0].DAU}|{week_diff_dau_feed}% \n\n \
        Только мессенджер: {dau_messages.iloc[-1].DAU}, \n Неделю назад: {dau_messages.iloc[0].DAU}|{week_diff_dau_messages}% \n \
        ----------------------\n\n \
        CTR: {round(feed.iloc[-1].CTR, 3)} \n Неделю назад: {round(feed.iloc[0].CTR, 3)}|{week_diff_ctr}% \n \
        ----------------------\n\n \
        Лайки: {likes_views.iloc[-1].users} \n Неделю назад: {likes_views.iloc[0].users}|{week_diff_likes}% \n \
        ----------------------\n\n \
        Просмотры: {likes_views.iloc[-2].users} \n Неделю назад: {likes_views.iloc[1].users}|{week_diff_views}% \n \
        ----------------------\n\n \
        Сообщения: {avg_messages.iloc[-1].messages} \n Неделю назад: {avg_messages.iloc[0].messages}|{week_diff_messages}%'
        
        return text
    
# соберем графики с метриками
    @task
    def chart_1(dau_both):
        fig, ax = plt.subplots(figsize=(9, 4))
        sns.lineplot(ax=ax, data=dau_both.iloc[1:], x='date', y='users')
        ax.set_title('DAU лента + мессенджер', fontsize=20)

        plot_object_1 = io.BytesIO()
        plt.savefig(plot_object_1)
        plot_object_1.seek(0)
        plot_object_1.name = 'report.png'
        plt.close()

        return plot_object_1

    @task
    def chart_2(feed, likes_views, retention): 
        fig, ax = plt.subplots(2, 2, figsize=(20, 10))
        fig.subplots_adjust(hspace=0.5)
        fig.suptitle('Лента. Данные за последние 7 дней', fontsize=20)

        sns.lineplot(ax=ax[0][0], data=feed.iloc[1:], x='date', y='DAU')
        ax[0][0].set_title('DAU лента', fontsize=16)

        sns.lineplot(ax=ax[0][1], data=feed.iloc[1:], x='date', y='CTR')
        ax[0][1].set_title('CTR', fontsize=16)

        sns.lineplot(ax=ax[1][0], data=likes_views.iloc[2:], x='date', y='users', hue='action')
        ax[1][0].set_ylabel('actions')
        ax[1][0].set_title('Лайки и просмотры', fontsize=16)

        sns.lineplot(ax=ax[1][1], data=retention, x='date', y='total_users', hue='source')
        ax[1][1].set_ylabel('users')
        ax[1][1].set_title('Retention за 7 дней', fontsize=16)

        plot_object_2 = io.BytesIO()
        plt.savefig(plot_object_2)
        plot_object_2.seek(0)
        plot_object_2.name = 'report2.png'
        plt.close()

        return plot_object_2

    @task
    def chart_3(avg_messages, dau_messages):    
        fig, ax = plt.subplots(1, 2, figsize=(25, 6))
        fig.subplots_adjust(hspace=0.5)
        fig.suptitle('Мессенджер. Данные за последние 7 дней', fontsize=20)

        sns.lineplot(ax=ax[0], data=dau_messages.iloc[1:], x='date', y='DAU')
        ax[0].set_title('DAU мессенджер', fontsize=16)

        sns.lineplot(ax=ax[1], data=avg_messages.iloc[1:], x='date', y='avg_messages')
        ax[1].set_title('Среднее кол-во сообщений на пользователя', fontsize=16)
        ax[1].set_ylabel('messages')

        plot_object_3 = io.BytesIO()
        plt.savefig(plot_object_3)
        plot_object_3.seek(0)
        plot_object_3.name = 'report3.png'
        plt.close()

        return plot_object_3
    
    @task
    def send_info(text, plot_object):
        bot.sendMessage(chat_id=chat_id, text=text)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_1)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_2)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_3)
        
    dau_both = extract_both()
    feed = extract_feed()
    likes_views = extract_views()
    retention = extract_retention()
    avg_messages = extract_messages()
    dau_messages = extract_messages_dau()
    text = message(dau_both, likes_views, feed, avg_messages, dau_messages)
    plot_object_1 = chart_1(dau_both)
    plot_object_2 = chart_2(feed, likes_views, retention)
    plot_object_3 = chart_3(avg_messages, dau_messages)
    send_info(text, plot_object_1, plot_object_2, plot_object_3)
        
dag_k_tomnikovskaja_11_7_3 = dag_k_tomnikovskaja_11_7_3()