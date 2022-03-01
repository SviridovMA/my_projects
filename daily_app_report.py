import pandas as pd
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse as ph
import io
import matplotlib.ticker as ticker

sns.set()


def get_plot(data_feed, data_msgs, data_new_users, data_dau_all):
    data = pd.merge(data_feed, data_msgs, on='date')
    data = pd.merge(data, data_dau_all, on='date')
    data = pd.merge(data, data_new_users, on='date')

    data['events_app'] = data['events'] + data['msgs']

    plot_objects = []

    fig, axes = plt.subplots(3, figsize=(10, 14))
    fig.suptitle('Статистика по всему приложению за 7 дней')
    app_dict = {
        0: {'y': ['events_app'], 'title': 'Events'},
        1: {'y': ['users', 'users_ios', 'users_android'], 'title': 'DAU'},
        2: {'y': ['new_users', 'new_users_ads', 'new_users_organic'], 'title': 'New users'}
    }

    for i in range(3):
        for y in app_dict[i]['y']:
            sns.lineplot(ax=axes[i], data=data, x='date', y=y)
        axes[i].set_title(app_dict[i]['title'])
        axes[i].set(xlabel=None)
        axes[i].set(ylabel=None)
        axes[i].legend(app_dict[i]['y'])
        for ind, label in enumerate(axes[i].get_xticklabels()):
            if ind % 3 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)
        axes[i].yaxis.set_major_formatter(ticker.EngFormatter())

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'app_stat.png'
    plot_object.seek(0)
    plt.close()

    plot_objects.append(plot_object)

    fig, axes = plt.subplots(2, 2, figsize=(14, 14))
    fig.suptitle('Статистика по ленте за 7 дней')
    plot_dict = {
        (0, 0): {'y': 'users_feed', 'title': 'Уникальные пользователи'},
        (0, 1): {'y': 'likes', 'title': 'Лайки'},
        (1, 0): {'y': 'views', 'title': 'Пользователи'},
        (1, 1): {'y': 'CTR', 'title': 'CTR'}
    }

    for i in range(2):
        for j in range(2):
            sns.lineplot(ax=axes[i, j], data=data, x='date', y=plot_dict[(i, j)]['y'])
            axes[i, j].set_title(plot_dict[(i, j)]['title'])
            axes[i, j].set(xlabel=None)
            axes[i, j].set(ylabel=None)
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

    plot_objects.append(plot_object)

    fig, axes = plt.subplots(3, figsize=(10, 14))
    fig.suptitle('Статистика по мессенджеру за 7 дней')
    msg_dict = {
        0: {'y': 'users_msg', 'title': 'DAU'},
        1: {'y': 'msgs', 'title': 'Messages'},
        2: {'y': 'MPU', 'title': 'Messages per user'}
    }

    for i in range(3):
        sns.lineplot(ax=axes[i], data=data, x='date', y=msg_dict[i]['y'])
        axes[i].set_title(msg_dict[i]['title'])
        axes[i].set(xlabel=None)
        axes[i].set(ylabel=None)
        for ind, label in enumerate(axes[i].get_xticklabels()):
            if ind % 3 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)
        axes[i].yaxis.set_major_formatter(ticker.EngFormatter())

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'msg_stat.png'
    plot_object.seek(0)
    plt.close()

    plot_objects.append(plot_object)

    return plot_objects


def app_report(chat=None):
    chat_id = chat or 636572045
    bot = telegram.Bot(token='5091102456:AAFP0AKJlraoYUUZft7qJocQZ3-TXPTHWGY')

    msg = '''Отчет по всему приложению за {date}
Events: {events:,}
👤 DAU: {users:,} ({to_users_day_ago} к дню назад, {to_users_week_ago} к неделе назад)
👤 DAU by platform:
    🍏 IOS users: {users_ios:,} ({to_users_ios_day_ago} к дню назад, {to_users_ios_week_ago} к неделе назад)
    🤖 Android users: {users_android:,} ({to_users_android_day_ago} к дню назад, {to_users_android_week_ago} к неделе назад)
👥 New users: {new_users:,} ({to_new_users_day_ago} к дню назад, {to_new_users_week_ago} к неделе назад)
👥 New users by source: 
    🧲 ads: {new_users_ads:,} ({to_new_users_ads_day_ago} к дню назад, {to_new_users_ads_week_ago} к неделе назад)
    🍀 organic: {new_users_organic:,} ({to_new_users_organic_day_ago} к дню назад, {to_new_users_organic_week_ago} к неделе назад)

📢📢📢📢 ЛЕНТА 📢📢📢📢: 
👤 DAU: {users_feed:,} ({to_users_feed_day_ago} к дню назад, {to_users_feed_week_ago} к неделе назад)
🧡 Likes: {likes:,} ({to_likes_day_ago} к дню назад, {to_likes_week_ago} к неделе назад)
👀 Views: {views:,} ({to_views_day_ago} к дню назад, {to_views_week_ago} к неделе назад)
🎯 CTR: {ctr:.2f}% ({to_ctr_day_ago} к дню назад, {to_ctr_week_ago} к неделе назад)
🖊 Posts: {posts:,} ({to_posts_day_ago} к дню назад, {to_posts_week_ago} к неделе назад)
💓 Likes per user: {lpu:.2} ({to_lpu_day_ago} к дню назад, {to_lpu_week_ago} к неделе назад)

📨📨📨📨 МЕССЕНДЖЕР 📨📨📨📨:
👤 DAU: {users_msg:,} ({to_users_msg_day_ago} к дню назад, {to_users_msg_week_ago} к неделе назад)
📩 Messages: {msgs:,} ({to_msgs_day_ago} к дню назад, {to_msgs_week_ago} к неделе назад)
👀 Messages per user: {mpu:.2f} ({to_mpu_day_ago} к дню назад, {to_mpu_week_ago} к неделе назад)
    '''

    connection = {'host': 'http://clickhouse.beslan.pro:8080',
                  'database': 'simulator',
                  'user': 'student',
                  'password': 'dpo_python_2020'
                  }

    query_feed = '''
            select 
                toDate(time) as date,
                uniqExact(user_id) as users_feed,
                countIf(user_id, action='like') as likes,
                countIf(user_id, action='view') as views,
                100 * likes / views as CTR,
                views + likes as events,
                uniqExact(post_id) as posts, 
                likes / users_feed as LPU
            from simulator.feed_actions
            where toDate(time) between today() - 8 and today() - 1
            group by date
            order by date
            '''
    data_feed = ph.read_clickhouse(query_feed, connection=connection)

    query_msgs = '''
            select 
                toDate(time) as date,
                uniqExact(user_id) as users_msg,
                count(user_id) as msgs,
                msgs / users_msg as MPU
            from simulator.message_actions
            where toDate(time) between today() - 8 and today() - 1
            group by date
            order by date
             '''
    data_msgs = ph.read_clickhouse(query_msgs, connection=connection)

    query_dau_all = '''
                select 
                    date,
                    uniqExact(user_id) as users,
                    uniqExactIf(user_id, os='iOS') as users_ios,
                    uniqExactIf(user_id, os='Android') as users_android
                from (
                    select distinct
                        toDate(time) as date,
                        user_id,
                        os
                    from simulator.feed_actions
                    where toDate(time) between today() - 8 and today() - 1
                    union all
                    select distinct
                        toDate(time) as date,
                        user_id,
                        os
                    from simulator.message_actions
                    where toDate(time) between today() - 8 and today() - 1
                ) as t
                group by date
             '''
    data_dau_all = ph.read_clickhouse(query_dau_all, connection=connection)

    query_new_users = '''
                select
                    date,
                    uniqExact(user_id) as new_users,
                    uniqExactIf(user_id, source='ads') as new_users_ads,
                    uniqExactIf(user_id, source='organic') as new_users_organic
                from (
                        select
                            user_id,
                            source,
                            min(min_dt) as date
                        from (
                                select 
                                    user_id,
                                    min(toDate(time)) as min_dt,
                                    source
                                from simulator.feed_actions
                                where toDate(time) between today() - 90 and today() - 1
                                group by user_id, source
                                union all
                                select 
                                    user_id,
                                    min(toDate(time)) as min_dt,
                                    source
                                from simulator.message_actions
                                where toDate(time) between today() - 90 and today() - 1
                                group by user_id, source
                        ) as t 
                        group by user_id, source
                ) as tab
                where date between today() - 8 and today() - 1
                group by date
                    '''
    data_new_users = ph.read_clickhouse(query_new_users, connection=connection)

    today = pd.Timestamp('now') - pd.DateOffset(days=1)
    day_ago = today - pd.DateOffset(days=1)
    week_ago = today - pd.DateOffset(days=7)

    data_feed['date'] = pd.to_datetime(data_feed['date']).dt.date
    data_msgs['date'] = pd.to_datetime(data_msgs['date']).dt.date
    data_dau_all['date'] = pd.to_datetime(data_dau_all['date']).dt.date
    data_new_users['date'] = pd.to_datetime(data_new_users['date']).dt.date

    data_feed = data_feed.astype(
        {'users_feed': int, 'likes': int, 'views': int, 'events': int, 'posts': int, 'CTR': float, 'LPU': float})
    data_msgs = data_msgs.astype({'users_msg': int, 'msgs': int, 'MPU': float})
    data_dau_all = data_dau_all.astype({'users': int, 'users_ios': int, 'users_android': int})
    data_new_users = data_new_users.astype({'new_users': int, 'new_users_ads': int, 'new_users_organic': int})

    def return_to_date(data, metrica, timestamp='day'):
        if timestamp == 'day':
            value = (data[data['date'] == today][metrica].iloc[0] -
                     data[data['date'] == day_ago][metrica].iloc[0]) / \
                    data[data['date'] == day_ago][metrica].iloc[0]
        elif timestamp == 'week':
            value = (data[data['date'] == today][metrica].iloc[0] -
                     data[data['date'] == week_ago][metrica].iloc[0]) / \
                    data[data['date'] == week_ago][metrica].iloc[0]
        else:
            raise 'Error'
        return str(round(100 * value, 2)) + '%'

    report = msg.format(

        date=today.date(),

        events=data_msgs[data_msgs['date'] == today.date()]['msgs'].iloc[0]
               + data_feed[data_msgs['date'] == today.date()]['events'].iloc[0],

        users=data_dau_all[data_dau_all['date'] == today.date()]['users'].iloc[0],
        to_users_day_ago=return_to_date(data=data_dau_all, metrica='users', timestamp='day'),
        to_users_week_ago=return_to_date(data=data_dau_all, metrica='users', timestamp='week'),

        users_ios=data_dau_all[data_dau_all['date'] == today.date()]['users_ios'].iloc[0],
        to_users_ios_day_ago=return_to_date(data=data_dau_all, metrica='users_ios', timestamp='day'),
        to_users_ios_week_ago=return_to_date(data=data_dau_all, metrica='users_ios', timestamp='week'),

        users_android=data_dau_all[data_dau_all['date'] == today.date()]['users_android'].iloc[0],
        to_users_android_day_ago=return_to_date(data=data_dau_all, metrica='users_android', timestamp='day'),
        to_users_android_week_ago=return_to_date(data=data_dau_all, metrica='users_android', timestamp='week'),

        new_users=data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0],
        to_new_users_day_ago=return_to_date(data=data_new_users, metrica='new_users', timestamp='day'),
        to_new_users_week_ago=return_to_date(data=data_new_users, metrica='new_users', timestamp='week'),

        new_users_ads=data_new_users[data_new_users['date'] == today.date()]['new_users_ads'].iloc[0],
        to_new_users_ads_day_ago=return_to_date(data=data_new_users, metrica='new_users_ads', timestamp='day'),
        to_new_users_ads_week_ago=return_to_date(data=data_new_users, metrica='new_users_ads', timestamp='week'),

        new_users_organic=data_new_users[data_new_users['date'] == today.date()]['new_users_organic'].iloc[0],
        to_new_users_organic_day_ago=return_to_date(data=data_new_users, metrica='new_users_organic', timestamp='day'),
        to_new_users_organic_week_ago=return_to_date(data=data_new_users, metrica='new_users_organic',
                                                     timestamp='week'),

        users_feed=data_feed[data_feed.date == today]['users_feed'].iloc[0],
        to_users_feed_day_ago=return_to_date(data=data_feed, metrica='users_feed', timestamp='day'),
        to_users_feed_week_ago=return_to_date(data=data_feed, metrica='users_feed', timestamp='week'),

        likes=data_feed[data_feed.date == today]['likes'].iloc[0],
        to_likes_day_ago=return_to_date(data=data_feed, metrica='likes', timestamp='day'),
        to_likes_week_ago=return_to_date(data=data_feed, metrica='likes', timestamp='week'),

        views=data_feed[data_feed.date == today]['views'].iloc[0],
        to_views_day_ago=return_to_date(data=data_feed, metrica='views', timestamp='day'),
        to_views_week_ago=return_to_date(data=data_feed, metrica='views', timestamp='week'),

        ctr=data_feed[data_feed.date == today]['CTR'].iloc[0],
        to_ctr_day_ago=return_to_date(data=data_feed, metrica='CTR', timestamp='day'),
        to_ctr_week_ago=return_to_date(data=data_feed, metrica='CTR', timestamp='week'),

        posts=data_feed[data_feed.date == today]['posts'].iloc[0],
        to_posts_day_ago=return_to_date(data=data_feed, metrica='posts', timestamp='day'),
        to_posts_week_ago=return_to_date(data=data_feed, metrica='posts', timestamp='week'),

        lpu=data_feed[data_feed.date == today]['LPU'].iloc[0],
        to_lpu_day_ago=return_to_date(data=data_feed, metrica='LPU', timestamp='day'),
        to_lpu_week_ago=return_to_date(data=data_feed, metrica='LPU', timestamp='week'),

        users_msg=data_msgs[data_msgs.date == today]['users_msg'].iloc[0],
        to_users_msg_day_ago=return_to_date(data=data_msgs, metrica='users_msg', timestamp='day'),
        to_users_msg_week_ago=return_to_date(data=data_msgs, metrica='users_msg', timestamp='week'),

        msgs=data_msgs[data_msgs.date == today]['msgs'].iloc[0],
        to_msgs_day_ago=return_to_date(data=data_msgs, metrica='msgs', timestamp='day'),
        to_msgs_week_ago=return_to_date(data=data_msgs, metrica='msgs', timestamp='week'),

        mpu=data_msgs[data_msgs.date == today]['MPU'].iloc[0],
        to_mpu_day_ago=return_to_date(data=data_msgs, metrica='MPU', timestamp='day'),
        to_mpu_week_ago=return_to_date(data=data_msgs, metrica='MPU', timestamp='week'),
    )

    bot.sendMessage(chat_id=chat_id, text=report)
    plot_objects = get_plot(data_feed, data_msgs, data_dau_all, data_new_users)
    for plot_object in plot_objects:
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)


if __name__ == '__main__':
    app_report()
