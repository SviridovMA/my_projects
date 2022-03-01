import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandahouse as ph
import seaborn as sns
import pandas as pd
import telegram
import io
from datetime import date, timedelta
from matplotlib.pyplot import figure

sns.set_style("darkgrid")
sns.set(rc={'figure.figsize':(11.7,8.27)})

# Инициализирую бота
bot = telegram.Bot(token=os.environ.get('REPORT_BOT_TOKEN'))

# Входные данные
a1 = 1 #коэффициент, задающий количество стандартных отклонений
a2 = 3 #коэффициент, задающий количество стандартных отклонений
start_date = str(date.today() - timedelta(days=7))end_date = str(date.today())
chat_id = -664596965
connection = {'host': 'http://clickhouse.beslan.pro:8080',
                      'database':'simulator',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }


def send_alert(df, metrica, method='quantile'):
    '''Высылает оповещение в тг если значение metrica в датасете df за самое последнее наблюдение выходит за границы
    Работает на основе правила сигм'''
    
    if metrica in ['likes', 'views', 'ctr', 'unique_users']:
        system = 'Лента новостей'
    else:
        system = 'Мессенджер'
    
    current_value = float(df[df.datetime == current_time][metrica])
    if method == 'quantile':
        left_conf = float(df[df.datetime == current_time]['left_conf_q_' + metrica])
        right_conf = float(df[df.datetime == current_time]['right_conf_q_' + metrica])
    elif method == 'sigm':
        left_conf = float(df[df.datetime == current_time]['left_conf_sigm_' + metrica])
        right_conf = float(df[df.datetime == current_time]['right_conf_sigm_' + metrica])
    else:
        print("""method must be either "quantile" or "sigm" """)
        raise ValueError 
    yesterday_value = float(df[df.datetime == current_time][metrica + '_y'])
    
    if (left_conf < current_value  < right_conf):
        print('Метрика {} не выходит за границы'.format(metrica))
    else:
        print('бибабоба')
        
        message = 'Аномалия в метрике {} за {} в системе {}\nТекущее значение - {}\nОтклонение от вчерашнего значения- {}\nВчерашнее значение - {}'.format(metrica,                                                                                                                                               current_time,                                                                                                                                               system,                                                                                                                                               round(current_value, 2),                                                                                                                                               str(round(abs(100 * (current_value / yesterday_value - 1)))) + '%',                                                                                                                                               round(yesterday_value, 2)
                                                                                                                                             )
        bot.sendMessage(chat_id=chat_id, text=message)
        
        plt.figure(figsize=(16, 12), dpi=80)
        fig, ax = plt.subplots()
        plt.xticks(rotation=46.6)
        plt.title(metrica)
        plt.xticks(range(0,len(df),10))

        ax.plot(df.datetime, df[metrica + '_w'], '-.', color="red" )
        ax.plot(df.datetime, df[metrica + '_y'], '--', color="orange" )
        ax.plot(df.datetime, df[metrica], '-', color="green" )
        if method == 'quantile':
            ax.fill_between(df.datetime, df['left_conf_q_' + metrica], df['right_conf_q_' + metrica], color='b', alpha=.4)
        else:
            ax.fill_between(df.datetime, df['left_conf_sigm_' + metrica], df['right_conf_sigm_' + metrica], color='b', alpha=.4)

        
        ax.legend([
            str(date.today() - timedelta(days=7)),
            str(date.today() - timedelta(days=1)),
            str(date.today()),
            'conf interval by {} method\na={}'.format(method, a)
        ])

        ax.set_xlabel('Время')
        ax.set_ylabel(metrica)
        if metrica != 'ctr':
            ax.yaxis.set_major_formatter(ticker.EngFormatter())
        plot_object = io.BytesIO()
        plt.tight_layout()
        plt.savefig(plot_object)
        plot_object.name = 'test_plot.png'
        plot_object.seek(0)

        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

# Выгружаю данные по ленте новостей
# За сегодня
query = '''
select 
    toStartOfFifteenMinutes(time) as datetime,
    countIf(action='like') as likes,
    countIf(action='view') as views,
    countIf(action='like') / countIf(action='view') as ctr,
    uniq(user_id) as unique_users
from simulator.feed_actions
where toDate(time) = today()
group by toStartOfFifteenMinutes(time)
order by toStartOfFifteenMinutes(time)
'''

feeds_today = ph.read_clickhouse(query, connection=connection)
feeds_today.drop(len(feeds_today) - 1, inplace=True)

current_time = str(feeds_today.datetime.max())[11:]

# За вчера
query = '''
select 
    toStartOfFifteenMinutes(time) as datetime,
    countIf(action='like') as likes_y,
    countIf(action='view') as views_y,
    countIf(action='like') / countIf(action='view') as ctr_y,
    uniq(user_id) as unique_users_y
from simulator.feed_actions
where today() - toDate(time) = 1
group by toStartOfFifteenMinutes(time)
order by toStartOfFifteenMinutes(time)
'''

feeds_yesterday = ph.read_clickhouse(query, connection=connection)
feeds_yesterday.head()

# Неделю назад
query = '''
select 
    toStartOfFifteenMinutes(time) as datetime,
    countIf(action='like') as likes_w,
    countIf(action='view') as views_w,
    countIf(action='like') / countIf(action='view') as ctr_w,
    uniq(user_id) as unique_users_w
from simulator.feed_actions
where today() - toDate(time) = 7
group by toStartOfFifteenMinutes(time)
order by toStartOfFifteenMinutes(time)
'''

feeds_lastweek = ph.read_clickhouse(query, connection=connection)
feeds_lastweek.head()

# Выгружаю средние значения основных метрик за прошлую неделю
query = '''
select 
    splitByChar(' ', toString(datetime))[2] as t,
    avg(t.likes) as mean_likes,
    avg(t.views) as mean_views,
    avg(t.ctr) as mean_ctr,
    avg(t.unique_users) as mean_unique_users,
    stddevPop(t.likes) as std_likes,
    stddevPop(t.views) as std_views,
    stddevPop(t.ctr)as std_ctr,
    stddevPop(t.unique_users) as std_unique_users,
    quantile(0.25)(t.likes) as q25_likes,
    quantile(0.25)(t.views) as q25_views,
    quantile(0.25)(t.ctr) as q25_ctr,
    quantile(0.25)(t.unique_users) as q25_unique_users,
    quantile(0.75)(t.likes) as q75_likes,
    quantile(0.75)(t.views) as q75_views,
    quantile(0.75)(t.ctr)as q75_ctr,
    quantile(0.75)(t.unique_users) as q75_unique_users
from 
    (select 
        toStartOfFifteenMinutes(time) as datetime,
        countIf(action='like') as likes,
        countIf(action='view') as views,
        countIf(action='like') / countIf(action='view') as ctr,
        uniq(user_id) as unique_users
    from simulator.feed_actions
    where today() - toDate(time) <= 7
    group by toStartOfFifteenMinutes(time)) t
group by t
order by t
'''

avg_values = ph.read_clickhouse(query, connection=connection)
avg_values.rename(columns={'t':'datetime'}, inplace=True)

for metrica in ['likes', 'views', 'ctr', 'unique_users']:
    avg_values['mean_' + metrica] = avg_values['mean_' + metrica].rolling(10, min_periods=1).mean()
    avg_values['std_' + metrica] = avg_values['std_' + metrica].rolling(10, min_periods=1).mean()
    avg_values['q25_' + metrica] = avg_values['q25_' + metrica].rolling(10, min_periods=1).mean()
    avg_values['q75_' + metrica] = avg_values['q75_' + metrica].rolling(10, min_periods=1).mean()

# Объединяю в одну таблицу

for df in [feeds_today, feeds_yesterday, feeds_lastweek]:
    df.datetime = df.datetime.astype('str')
    df.datetime = df.datetime.apply(lambda x:x[11:])

feeds = feeds_today.merge(feeds_yesterday, on=['datetime'], how='outer').merge(feeds_lastweek, on=['datetime'], how='outer').merge(avg_values, on=['datetime'], how='outer').fillna(0)

for metrica in ['likes', 'views', 'ctr', 'unique_users']:
    if metrica in ['likes', 'views']:
        a = a1
    else:
        a = a2
    feeds['left_conf_sigm_' + metrica] = feeds['mean_' + metrica] - a * feeds['std_' + metrica]
    feeds['right_conf_sigm_' + metrica] = feeds['mean_' + metrica] + a * feeds['std_' + metrica]                                                                    
    feeds.drop(['mean_' + metrica, 'std_' + metrica], axis=1, inplace=True)
    
    feeds['left_conf_q_' + metrica] = feeds['q25_' + metrica] - a * (feeds['q75_' + metrica] - feeds['q25_' + metrica])
    feeds['right_conf_q_' + metrica] = feeds['q75_' + metrica] + a * (feeds['q75_' + metrica] - feeds['q25_' + metrica])                                                                   
    feeds.drop(['q75_' + metrica, 'q25_' + metrica], axis=1, inplace=True)
    
# Проверяю метрики

for metrica in ['likes', 'views', 'ctr', 'unique_users']:
    send_alert(feeds, metrica, method='quantile')

# Выгружаю данные по месенджеру
# За сегодня
query = '''
select
    toStartOfFifteenMinutes(time) as datetime,
    uniq(user_id) as unique_users,
    count(reciever_id) as messages_sent
from simulator.message_actions
where toDate(time) = today()
group by toStartOfFifteenMinutes(time)
order by toStartOfFifteenMinutes(time)
'''

mess_today = ph.read_clickhouse(query, connection=connection)
mess_today.drop(len(mess_today) - 1, inplace=True)

# За вчера
query = '''
select
    toStartOfFifteenMinutes(time) as datetime,
    uniq(user_id) as unique_users_y,
    count(reciever_id) as messages_sent_y
from simulator.message_actions
where today() - toDate(time) = 1
group by toStartOfFifteenMinutes(time)
order by toStartOfFifteenMinutes(time)
'''

mess_yesterday = ph.read_clickhouse(query, connection=connection)

# Неделю назад
query = '''
select
    toStartOfFifteenMinutes(time) as datetime,
    uniq(user_id) as unique_users_w,
    count(reciever_id) as messages_sent_w
from simulator.message_actions
where today() - toDate(time) = 7
group by toStartOfFifteenMinutes(time)
order by toStartOfFifteenMinutes(time)
'''

mess_lastweek = ph.read_clickhouse(query, connection=connection)

# Выгружаю средние значения основных метрик за прошлую неделю
query = '''
select 
    splitByChar(' ', toString(datetime))[2] as t,
    avg(t.messages_sent) as mean_messages_sent,
    avg(t.unique_users) as mean_unique_users,
    stddevPop(t.messages_sent)as std_messages_sent,
    stddevPop(t.unique_users) as std_unique_users,
    quantile(0.25)(t.messages_sent) as q25_messages_sent,
    quantile(0.25)(t.unique_users) as q25_unique_users,
    quantile(0.75)(t.messages_sent)as q75_messages_sent,
    quantile(0.75)(t.unique_users) as q75_unique_users
from 
    (select 
        toStartOfFifteenMinutes(time) as datetime,
        uniq(user_id) as unique_users,
        count(reciever_id) as messages_sent
    from simulator.message_actions
    where today() - toDate(time) <= 7
    group by toStartOfFifteenMinutes(time)) t
group by t
order by t
'''

avg_values = ph.read_clickhouse(query, connection=connection)
avg_values.rename(columns={'t':'datetime'}, inplace=True)

for metrica in ['unique_users', 'messages_sent']:
    avg_values['mean_' + metrica] = avg_values['mean_' + metrica].rolling(10, min_periods=1).mean()
    avg_values['std_' + metrica] = avg_values['std_' + metrica].rolling(10, min_periods=1).mean()
    avg_values['q25_' + metrica] = avg_values['q25_' + metrica].rolling(10, min_periods=1).mean()
    avg_values['q75_' + metrica] = avg_values['q75_' + metrica].rolling(10, min_periods=1).mean()

# Объединяю в одну таблицу

for df in [mess_today, mess_yesterday, mess_lastweek]:
    df.datetime = df.datetime.astype('str')
    df.datetime = df.datetime.apply(lambda x:x[11:])

mess = mess_today.merge(mess_yesterday, on=['datetime'], how='outer').merge(mess_lastweek, on=['datetime'], how='outer').merge(avg_values, on=['datetime'], how='outer').fillna(0)

for metrica in ['unique_users', 'messages_sent']:
    a = a2
    mess['left_conf_' + metrica] = mess['mean_' + metrica] - a * mess['std_' + metrica]
    mess['right_conf_' + metrica] = mess['mean_' + metrica] + a * mess['std_' + metrica]                                                                    
    mess.drop(['mean_' + metrica, 'std_' + metrica], axis=1, inplace=True)
    
    mess['left_conf_q_' + metrica] = mess['q25_' + metrica] - a * (mess['q75_' + metrica] - mess['q25_' + metrica])
    mess['right_conf_q_' + metrica] = mess['q75_' + metrica] + a * (mess['q75_' + metrica] - mess['q25_' + metrica])                                                                   
    mess.drop(['q75_' + metrica, 'q25_' + metrica], axis=1, inplace=True)


# Проверяю метрики
for metrica in ['unique_users', 'messages_sent']:
    send_alert(mess, metrica, method='quantile')




