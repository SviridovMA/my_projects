import pandas as pd
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse as ph
import io
import matplotlib.ticker as ticker
import os

from datetime import date, timedelta, datetime

sns.set()

def get_plot(data):
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Статистика по ленте за предыдущие 7 дней')

    plot_dict = {
        (0, 0): {'y':'DAU', 'title':'Уникальные пользователи'},
        (0, 1): {'y': 'likes', 'title': 'Лайки'},
        (1, 0): {'y': 'views', 'title': 'Пользователи'},
        (1, 1): {'y': 'CTR', 'title': 'CTR'}
                 }

    for i in range(2):
        for j in range(2):
            sns.lineplot(ax=axes[i, j], data=data, x='date', y=plot_dict[(i, j)]['y'])
            axes[i, j].set_title(plot_dict[(i, j)]['title'])
            axes[i, j].set(xlabel = None)
            axes[i, j].set(ylabel = None)
            for ind, label in enumerate(axes[i, j].get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            axes[i, j].yaxis.set_major_formatter(ticker.EngFormatter())


    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'feed_stat.png'
    plot_object.seek(0)
    plt.close()


    return plot_object

def feed_report(chat=None):

    chat_id = chat or 636572045
    bot = telegram.Bot(token=os.environ.get('REPORT_BOT_TOKEN'))
    msg = '''🗓 Отчет по ленте за {date} 🗓
All Events: {events:,} 
👤 DAU: {users:,} ({to_users_day_ago} к дню назад, {to_users_week_ago} к неделе назад)
🧡 Likes: {likes:,} ({to_likes_day_ago} к дню назад, {to_likes_week_ago} к неделе назад)
👀 Views: {views:,} ({to_views_day_ago} к дню назад, {to_views_week_ago} к неделе назад)
🎯 CTR: {ctr:.2f}% ({to_ctr_day_ago} к дню назад, {to_ctr_week_ago} к неделе назад)
Posts: {posts:,} ({to_posts_day_ago} к дню назад, {to_posts_week_ago} к неделе назад)
Likes per user: {lpu:.2} ({to_lpu_day_ago} к дню назад, {to_lpu_week_ago} к неделе назад)
    '''

    query = '''
        select 
            toDate(time) as date,
            uniqExact(user_id) as DAU,
            countIf(user_id, action='like') as likes,
            countIf(user_id, action='view') as views,
            100 * likes / views as CTR,
            views + likes as events,
            uniqExact(post_id) as posts, 
            likes / DAU as LPU
        from simulator.feed_actions
        where toDate(time) between today() -8 and today() - 1
        group by date
        order by date
        '''

    connection = {'host': 'http://clickhouse.beslan.pro:8080',
                  'database': 'simulator',
                  'user': 'student',
                  'password': 'dpo_python_2020'
                  }

    data = ph.read_clickhouse(query, connection=connection)

    today = pd.Timestamp(date.today() - timedelta(days=1))
    day_ago = pd.Timestamp(date.today() - timedelta(days=2))
    week_ago = pd.Timestamp(date.today() - timedelta(days=8))

    def return_to_date(metrica, timestamp='day'):
        if timestamp == 'day':
            value = (int(data[data['date'] == today][metrica].iloc[0]) -
                     int(data[data['date'] == day_ago][metrica].iloc[0])) / \
                    int(data[data['date'] == day_ago][metrica].iloc[0])
        elif timestamp == 'week':
            value = (int(data[data['date'] == today][metrica].iloc[0]) -
                     int(data[data['date'] == week_ago][metrica].iloc[0])) / \
                    int(data[data['date'] == week_ago][metrica].iloc[0])
        else:
            raise 'Error'
        return str(round(100 * value, 2)) + '%'


    report = msg.format(date=today.date(),

                        events=data[data.date == today]['events'].iloc[0],

                        users=data[data.date == today]['DAU'].iloc[0],
                        to_users_day_ago=return_to_date('DAU', 'day'),
                        to_users_week_ago=return_to_date('DAU', 'week'),

                        likes=data[data.date == today]['likes'].iloc[0],
                        to_likes_day_ago=return_to_date('likes', 'day'),
                        to_likes_week_ago=return_to_date('likes', 'week'),

                        views=data[data.date == today]['views'].iloc[0],
                        to_views_day_ago=return_to_date('views', 'day'),
                        to_views_week_ago=return_to_date('views', 'week'),

                        ctr=data[data.date == today]['CTR'].iloc[0],
                        to_ctr_day_ago=return_to_date('CTR', 'day'),
                        to_ctr_week_ago=return_to_date('CTR', 'week'),

                        posts=data[data.date == today]['posts'].iloc[0],
                        to_posts_day_ago=return_to_date('posts', 'day'),
                        to_posts_week_ago=return_to_date('posts', 'week'),

                        lpu=data[data.date == today]['LPU'].iloc[0],
                        to_lpu_day_ago=return_to_date('LPU', 'day'),
                        to_lpu_week_ago=return_to_date('LPU', 'week')
                        )
    bot.sendMessage(chat_id=chat_id, text=report)
    bot.sendPhoto(chat_id=chat_id, photo=get_plot(data))


if __name__ == '__main__':
    feed_report()
