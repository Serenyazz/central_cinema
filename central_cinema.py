import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def calculate_metrics(user, password, host, today):
    conn = psycopg2.connect(
        dbname='courses'
        , user=user # твой юзер
        , password=password # твой пароль
        , target_session_attrs='read-write'
        , host=host
        , port='5432'
    )

    cur = conn.cursor()

    cur = conn.cursor()
    cur.execute("select version();")
    cur.fetchall()


    # users 
    cur = conn.cursor()
    cur.execute("SELECT user_id, created_at, cogort FROM python_for_da.central_cinema_users ")
    users_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    users = pd.DataFrame(users_sql, columns = colnames)
    users.head()

    # user_payments
    cur = conn.cursor()
    cur.execute("SELECT * FROM python_for_da.central_cinema_user_payments")
    user_payments_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    user_payments = pd.DataFrame(user_payments_sql, columns = colnames)
    user_payments.head()

    # partner_comission
    cur = conn.cursor()
    cur.execute("SELECT partner_id, commission FROM python_for_da.central_cinema_partner_commission")
    partner_comission_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    partner_comission = pd.DataFrame(partner_comission_sql, columns = colnames)
    partner_comission.head()

    # user_activity
    cur = conn.cursor()
    cur.execute('''
                SELECT user_id, title_id, play_start, play_end 
                FROM python_for_da.central_cinema_user_activity 
                WHERE DATE(play_start) <= CURRENT_DATE - INTERVAL '1 DAY'
                    and DATE(play_start) >= CURRENT_DATE - INTERVAL '3 MONTH'- INTERVAL '2 DAY'; ''')    
    user_activity_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    user_activity = pd.DataFrame(user_activity_sql, columns = colnames)
    user_activity.head()

    # title
    cur = conn.cursor()
    cur.execute("""select distinct t.title_id, t.duration
                    from python_for_da.central_cinema_title t 
                    join python_for_da.central_cinema_user_activity a on 
                    t.title_id = a.title_id and play_start>'2024-08-20'
    """)
    title_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    title = pd.DataFrame(title_sql, columns = colnames)

    # partner_dict
    cur = conn.cursor()
    cur.execute("SELECT * FROM python_for_da.central_cinema_partner_dict")
    partner_comission_sql = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    partner_dict = pd.DataFrame(partner_comission_sql, columns = colnames)

    # Нужные переменные 
    today_str = today 
    today = datetime.strptime(today, '%Y-%m-%d')
    yesterday = datetime.strptime((today-timedelta(days = 1)).strftime('%Y-%m-%d'),'%Y-%m-%d')
    yesterday_last_month = datetime.strptime((today-relativedelta(months = 1)-timedelta(days = 1)).strftime('%Y-%m-%d'),'%Y-%m-%d')
    yesterday_prev_month = datetime.strptime((today-relativedelta(months = 2)-timedelta(days = 1)).strftime('%Y-%m-%d'),'%Y-%m-%d')   

    # кол-во триалов
    user_payments['payment_day'] = pd.to_datetime(user_payments['payment_date']).dt.to_period('D').dt.to_timestamp()
    trial_yesterday = user_payments[(user_payments['is_trial']==1)&(user_payments['payment_day']==yesterday)]['user_payment_id'].count()
    trial_yesterday_last_month = user_payments[(user_payments['is_trial']==1)&(user_payments['payment_day']==yesterday_last_month)]['user_payment_id'].count()
    trial_delta_trial = round((trial_yesterday)/trial_yesterday_last_month*100,2)
    symbol_trial_delta_trial = "\U0001F4C8"+"\U00002705" if trial_delta_trial > 100 else "\U0001F4C8 \U0001F53B"
    
    # кол-во оплат
    user_payments['payment_day'] = pd.to_datetime(user_payments['payment_date']).dt.to_period('D').dt.to_timestamp()
    non_trial_payment_yesterday = user_payments[(user_payments['is_trial']==0)&(user_payments['payment_day']==yesterday)]['user_payment_id'].count()
    non_trial_yesterday_last_month = user_payments[(user_payments['is_trial']==0)&(user_payments['payment_day']==yesterday_last_month)]['user_payment_id'].count()
    trial_delta_payments = round((non_trial_payment_yesterday)/non_trial_yesterday_last_month*100,2)
    symbol_trial_delta_payments = "\U0001F4C8"+"\U00002705" if trial_delta_payments > 100 else "\U0001F4C8 \U0001F53B"

    # конверсия в первую оплату 
    # 1
    mask_yesterday_last_month_trial = (user_payments['is_trial']==1)&(user_payments['payment_day']==yesterday_last_month) # Условие для знаменателя
    mask_yesterday_non_trial = (user_payments['is_trial']==0)&(user_payments['payment_day']==yesterday) # Условие для числитель
    trial_yesterday_last_month_id = user_payments[mask_yesterday_last_month_trial]['user_id'] # Знаменатель
    non_trial_yesterday = user_payments[mask_yesterday_non_trial&(user_payments['user_id'].isin(trial_yesterday_last_month_id))]['user_id'] # числитель
    conversion_cur_month = non_trial_yesterday.count() / trial_yesterday_last_month_id.count() # Конверсия текущего месяца
    conversion_cur_month

    # 2
    mask_yesterday_prev_month_trial = (user_payments['is_trial']==1)&(user_payments['payment_day']==yesterday_prev_month) # Условие для знаменателя
    mask_last_month_non_trial = (user_payments['is_trial']==0)&(user_payments['payment_day']==yesterday_last_month)
    trial_yesterday_prev_month_trial = user_payments[mask_yesterday_prev_month_trial]['user_id'] # Знаменатель
    non_trial_last_month = user_payments[mask_last_month_non_trial&(user_payments['user_id'].isin(trial_yesterday_prev_month_trial))]['user_id'] # числитель
    conversion_last_month = non_trial_last_month.count() / trial_yesterday_prev_month_trial.count() # Конверсия прошлого месяца

    # 3
    trial_delta_conv = round((conversion_cur_month)/conversion_last_month*100,2)
    symbol_trial_delta_conv = "\U0001F4C8"+"\U00002705" if trial_delta_conv > 100 else "\U0001F4C8 \U0001F53B"

    # Валовый Cash-in
    cashin_non_trial_yesterday = user_payments[mask_yesterday_non_trial]['user_id'].count() * 299
    cashin_non_trial_last_month = user_payments[mask_last_month_non_trial]['user_id'].count() * 299
    trial_delta_cash_in = round((cashin_non_trial_yesterday)/cashin_non_trial_last_month*100,2)
    symbol_trial_delta_cash_in = "\U0001F4C8"+"\U00002705" if trial_delta_cash_in > 100 else "\U0001F4C8 \U0001F53B"

    #CAC
    # Your code here
    mask_yesterday_trial = (user_payments['is_trial']==1)&(user_payments['payment_day']<yesterday)
    mask_yesterday_last_month_trial = (user_payments['is_trial']==1)&(user_payments['payment_day']<yesterday_last_month)
    partner_comission_yesterday_trial = partner_comission.merge(user_payments[mask_yesterday_trial], on='partner_id', how='inner')
    partner_comission_yesterday_last_month_trial = partner_comission.merge(user_payments[mask_yesterday_last_month_trial], on='partner_id', how='inner')
    cac_yesterday_trial = partner_comission_yesterday_trial['commission'].mean()
    cac_yesterday_last_month_trial = partner_comission_yesterday_last_month_trial['commission'].mean()
    trial_delta_cac = round((cac_yesterday_trial)/cac_yesterday_last_month_trial*100,2)
    symbol_trial_delta_cac = "\U0001F4C8"+"\U00002705" if trial_delta_cac > 100 else "\U0001F4C8 \U0001F53B"


    # Сдвигаем значения в дф для ltr
    def shift_left(row):
        non_zeros = row[row != 0]
        return pd.Series(list(non_zeros) + [0] * (len(row) - len(non_zeros)))

    # Прогнозируемый LTR
    arpu = 299

    new_users = users[['user_id', 'created_at']].copy()
    new_users['reg_month_user'] = users['created_at'].dt.to_period('M')#.dt.to_timestamp()

    new_user_payments = user_payments[['user_id', 'payment_date', 'payment_day']].copy()
    new_user_payments['payment_month'] = new_user_payments['payment_date'].dt.to_period('M')#.dt.to_timestamp()

    users_and_payments = new_users[['user_id', 'reg_month_user']].merge(new_user_payments[['user_id', 'payment_month']], how='inner', on='user_id')

    lt_calculate = users_and_payments.groupby(['reg_month_user', 'payment_month']).agg(amount=('user_id', 'count')).unstack(fill_value=0)
    lt_calculate.columns = lt_calculate.columns.droplevel(0)

    # # Маски для когорт
    # mask_until_matured_cogort_yesterday_columns = lt_calculate.columns[~(lt_calculate.columns < until_matured_cogort_yesterday)]
    mask_until_matured_cogort_yesterday_index = lt_calculate.index[~(lt_calculate.index < pd.to_datetime(yesterday).to_period('M'))]

    # mask_until_matured_cogort_yesterday_last_month_columns = lt_calculate.columns[~(lt_calculate.columns < until_matured_cogort_yesterday_last_month)]
    mask_until_matured_cogort_yesterday_last_month_index = lt_calculate.index[~(lt_calculate.index < pd.to_datetime(yesterday_last_month).to_period('M'))]

    yesterday_lt_calculate = lt_calculate.copy()
    yesterday_last_month_lt_calculate = lt_calculate.copy()

    # Дропаем когорты
    yesterday_lt_calculate.drop(mask_until_matured_cogort_yesterday_index, inplace=True)  # дропаем т.к они не полные
    yesterday_last_month_lt_calculate.drop(mask_until_matured_cogort_yesterday_last_month_index, inplace=True) 
    
    # переворачиваем данные для простоты работы
    yesterday_lt_calculate = yesterday_lt_calculate.apply(shift_left, axis=1)
    yesterday_last_month_lt_calculate = yesterday_last_month_lt_calculate.apply(shift_left, axis=1)

    right_border1 = max(yesterday_lt_calculate.columns)
    right_border2 = max(yesterday_last_month_lt_calculate.columns)

    # считаем ретеншн
    for first, second in zip(range(0, right_border1 + 1), range(0, right_border2 + 1)):
        yesterday_lt_calculate[f'ret_{first}'] = yesterday_lt_calculate.iloc[:, first] / yesterday_lt_calculate.iloc[:, 0]
        yesterday_last_month_lt_calculate[f'ret_{second}'] = yesterday_last_month_lt_calculate.iloc[:, second] / yesterday_last_month_lt_calculate.iloc[:, 0]

    yesterday_lt_calculate['lt']  = yesterday_lt_calculate.iloc[:, right_border1+1:].sum(axis=1)
    yesterday_last_month_lt_calculate['lt']  = yesterday_last_month_lt_calculate.iloc[:, right_border2+1:].sum(axis=1)

    avg_lt1 = (np.dot(yesterday_lt_calculate['lt'], yesterday_lt_calculate[0]) / yesterday_lt_calculate[0].sum()) * arpu
    avg_lt2 = (np.dot(yesterday_last_month_lt_calculate['lt'], yesterday_last_month_lt_calculate[0]) / yesterday_last_month_lt_calculate[0].sum()) * arpu

    trial_delta_ltr = round((avg_lt1)/avg_lt2*100,2)
    symbol_trial_delta_ltr = "\U0001F4C8"+"\U00002705" if trial_delta_ltr > 100 else "\U0001F4C8 \U0001F53B"


    # Прогнозируемый LTV
    ltv1 = avg_lt1 - cac_yesterday_trial
    ltv2 = avg_lt2 - cac_yesterday_last_month_trial

    trial_delta_ltv = round((ltv1)/ltv2*100,2)
    symbol_trial_delta_ltv = "\U0001F4C8"+"\U00002705" if trial_delta_ltv > 100 else "\U0001F4C8 \U0001F53B"

    # Среднее время сессии
    new_activ = user_activity[['user_id', 'play_start', 'play_end', 'title_id']].copy()
    new_activ['day'] = pd.to_datetime(new_activ['play_start']).dt.to_period('D').dt.to_timestamp()
    new_activ['month'] = new_activ['play_start'].dt.to_period('M')
    new_activ['watch_time'] = (new_activ['play_end'] - new_activ['play_start']).dt.seconds / 60

    new_activ_yesterday = new_activ[ new_activ['day'] == yesterday ]
    new_acti_last_month = new_activ[ new_activ['day'] == yesterday_last_month ]

    new_activ_yesterday_mean = new_activ_yesterday['watch_time'].mean()
    new_acti_last_month_mean = new_acti_last_month['watch_time'].mean()

    trial_delta_ses = round((new_activ_yesterday_mean)/new_acti_last_month_mean*100,2)
    symbol_trial_delta_ses = "\U0001F4C8"+"\U00002705" if trial_delta_ses > 100 else "\U0001F4C8 \U0001F53B"

    # Досматреваемость 
    new_activ_and_title_yesterday = new_activ_yesterday.merge(title, how='inner', on='title_id')
    new_activ_and_title_last_month = new_acti_last_month.merge(title, how='inner', on='title_id')

    avg_watched1 = round(new_activ_and_title_yesterday['watch_time'].mean() / new_activ_and_title_yesterday['duration'].mean(), 2)
    avg_watched2 = round(new_activ_and_title_last_month['watch_time'].mean() / new_activ_and_title_last_month['duration'].mean(), 2)

    trial_delta_watched = round((avg_watched1)/avg_watched2*100,2)
    symbol_trial_delta_watched = "\U0001F4C8"+"\U00002705" if trial_delta_watched > 100 else "\U0001F4C8 \U0001F53B"

    # Уникальное число смотервших 
    unique_watched_yesterday = new_activ[ new_activ['day'] == yesterday ]['user_id'].nunique() 
    unique_watched_last_month = new_activ[ new_activ['day'] == yesterday_last_month ]['user_id'].nunique()

    trial_delta_unique_watched = round((unique_watched_yesterday)/unique_watched_last_month*100,2)
    symbol_trial_delta_unique_watched = "\U0001F4C8"+"\U00002705" if trial_delta_unique_watched > 100 else "\U0001F4C8 \U0001F53B"


    message = f"""
Central Cinema 🍿
на {today_str}
вчера (в прошлом месяце)

🔷Количество триалов
{trial_yesterday} ({trial_yesterday_last_month})
МoМ %: {symbol_trial_delta_trial} {trial_delta_trial}%

🔷Количество оплат
{non_trial_payment_yesterday} ({non_trial_yesterday_last_month})
МoМ %: {symbol_trial_delta_payments} {trial_delta_payments}%

🔷Конверсия в первую оплату%
{round(conversion_cur_month, 2)}  ({round(conversion_last_month, 2)})
МoМ %: {symbol_trial_delta_conv} {trial_delta_conv}%

💰Валовый cash-in
{cashin_non_trial_yesterday}  ({cashin_non_trial_last_month})
МoМ %: {symbol_trial_delta_cash_in} {trial_delta_cash_in}%

💰CAC
{round(cac_yesterday_trial, 2)} ({round(cac_yesterday_last_month_trial, 2)})
МoМ %: {symbol_trial_delta_cac} {trial_delta_cac}%

💰Прогнозируемый LTR
{round(avg_lt1, 2)} ({round(avg_lt2, 2)})
МoМ %: {symbol_trial_delta_ltr} {trial_delta_ltr}%

💰Прогнозируемый LTV
{round(ltv1, 2)} ({round(ltv2, 2)})
МoМ %: {symbol_trial_delta_ltv} {trial_delta_ltv}%

⏱️Среднее время сессии (мин.)
{round(new_activ_yesterday_mean, 2)} ({round(new_acti_last_month_mean, 2)})
МoМ %: {symbol_trial_delta_ses} {trial_delta_ses}%

⏱️Досматриваемость
{avg_watched1} ({avg_watched2})
МoМ %: {symbol_trial_delta_watched} {trial_delta_watched}%

👀Уникальное количество смотревших
{unique_watched_yesterday} ({unique_watched_last_month})
МoМ %: {symbol_trial_delta_unique_watched} {trial_delta_unique_watched}%
    """
    # Кол-во триалов
    cnt_trial_last_month = user_payments[(user_payments['is_trial']==1)&(user_payments['payment_day']>yesterday_last_month)&(user_payments['payment_day']<=yesterday)]
    trial_amount = cnt_trial_last_month.groupby('payment_day', as_index=False).agg(amount_trial=('user_id', 'count'))

    # Кол-во оплат
    cnt_non_trial_last_month = user_payments[(user_payments['is_trial']==0)&(user_payments['payment_day']>yesterday_last_month)&(user_payments['payment_day']<=yesterday)]
    grouped_cnt_non_trial_last_month = cnt_non_trial_last_month.groupby('payment_day', as_index=False).agg(amount_non_trial=('user_id', 'count'))

    # Конверсия в первую оплату
    mask_yesterday_prev_month_trial = (user_payments['is_trial']==1)&(user_payments['payment_day']>yesterday_prev_month)&(user_payments['payment_day']<=yesterday_last_month) # Условие для знаменателя
    mask_last_month_non_trial = (user_payments['is_trial']==0)&(user_payments['payment_day']>yesterday_last_month)&(user_payments['payment_day']<=yesterday)

    usl1 = user_payments[mask_yesterday_prev_month_trial]
    usl1['payment_day'] = usl1['payment_day'].dt.day
    pre_res_usl1 = usl1.groupby('payment_day', as_index=False).agg(cnt=('user_id', 'count'))

    usl2 = user_payments[mask_last_month_non_trial&(user_payments['user_id'].isin(usl1['user_id']))] 
    usl2['payment_day'] = usl2['payment_day'].dt.day
    pre_res_usl2 = usl2.groupby('payment_day', as_index=False).agg(cnt=('user_id', 'count'))

    merged = pre_res_usl1.merge(pre_res_usl2, how='inner', on='payment_day', suffixes=['_trial', '_non_trial'])
    merged['conversion'] = merged['cnt_non_trial'] / merged['cnt_trial']

    # валовый cash-in
    grouped_cnt_non_trial_last_month['cash_in'] = grouped_cnt_non_trial_last_month['amount_non_trial'] * 299
    grouped_cnt_non_trial_last_month.reset_index(inplace=True)


    # cac ltv 
    CAC_avg_yesterday = cac_yesterday_trial
    CAC_avg_yesterday_last_month = cac_yesterday_last_month_trial

    LTV_yesterday = ltv1
    LTV_yesterday_last_month = ltv2

    CAC = [CAC_avg_yesterday, CAC_avg_yesterday_last_month]
    LTV = [LTV_yesterday, LTV_yesterday_last_month]

    # Среднее время сессии
    new_activ = user_activity[['user_id', 'play_start', 'play_end', 'title_id']].copy()
    new_activ['day'] = pd.to_datetime(new_activ['play_start']).dt.to_period('D').dt.to_timestamp()
    new_activ['month'] = new_activ['play_start'].dt.to_period('M')
    new_activ['watch_time'] = (new_activ['play_end'] - new_activ['play_start']).dt.seconds / 60

    last_month_new_activ = new_activ[(new_activ['day']>yesterday_last_month)&(new_activ['day']<=yesterday)]
    avg_last_month_watch_time = last_month_new_activ.groupby('day', as_index=False).agg(avg_watch_time=('watch_time', 'mean'))

    # доссматриваемость
    new_activ_and_title_last_month = last_month_new_activ.merge(title, how='inner', on='title_id')
    dosmtr = new_activ_and_title_last_month.groupby('day').agg(avg_watch_time=('watch_time', 'mean'),
                                                    avg_duration=('duration', 'mean') )
    dosmtr['dosmtr'] = dosmtr['avg_watch_time'] / dosmtr['avg_duration']
    dosmtr.reset_index(inplace=True)

    # Уникальное кол-во смотревших
    users_watched_last_month = new_activ[(new_activ['day']>yesterday_last_month)&(new_activ['day']<=yesterday)]
    unique_users_watched_last_month = users_watched_last_month.groupby('day', as_index=False).agg(uniq_users=('user_id', 'nunique'))

    simple_line_graph('Кол-во триалов', trial_amount['payment_day'], trial_amount['amount_trial'])  # кол-во триалы
    simple_line_graph('cash in', grouped_cnt_non_trial_last_month['payment_day'], grouped_cnt_non_trial_last_month['cash_in'])  # cash in
    simple_line_graph('уникальное кол-во смотрящих', unique_users_watched_last_month['day'], unique_users_watched_last_month['uniq_users'])  # уник кол-во сомтр
    combined_graph('Кол-во оплат', grouped_cnt_non_trial_last_month['payment_day'],  grouped_cnt_non_trial_last_month['amount_non_trial'], merged['conversion'])
    combined_graph('Среднее время просмотра + досматриваемость', avg_last_month_watch_time['day'], avg_last_month_watch_time['avg_watch_time'], dosmtr['dosmtr'])
    plot_cac_ltv_ltr(CAC, LTV)

    # ДОП МЕТРИКИ

    # Зарегистрировались два месяца назад (is_trial=1) и оформили платную подписку в прошлом месяце
    cohort_two_months_ago = user_payments[
        (user_payments['payment_date'].dt.to_period('M') == pd.to_datetime(yesterday_prev_month).to_period('M')) &
        (user_payments['is_trial'] == 1)
    ]

    # Активность пользователей из этой когорты в прошлом месяце
    active_last_month = user_payments[
        (user_payments['payment_date'].dt.to_period('M') == pd.to_datetime(yesterday_last_month).to_period('M')) &
        (user_payments['user_id'].isin(cohort_two_months_ago['user_id']))
    ]

    # Группируем данные по партнёрам
    active_last_month_counts = active_last_month.groupby('partner_id').size().reset_index(name='active_users_last_month')

    # Аналогично, считаем активность когорты три месяца назад в позапрошлом месяце
    cohort_three_months_ago = user_payments[
        (user_payments['payment_date'].dt.to_period('M') == pd.to_datetime(yesterday_prev_month).to_period('M') - 1 ) &
        (user_payments['is_trial'] == 1)
    ]

    active_two_months_ago = user_payments[
        (user_payments['payment_date'].dt.to_period('M') == pd.to_datetime(yesterday_prev_month).to_period('M')) &
        (user_payments['user_id'].isin(cohort_three_months_ago['user_id']))
    ]

    active_two_months_ago_counts = active_two_months_ago.groupby('partner_id').size().reset_index(name='active_users_two_months_ago')
    partner_activity = pd.merge(active_last_month_counts, active_two_months_ago_counts, on='partner_id', how='outer').fillna(0)
    partner_activity = partner_activity.merge(partner_dict, on='partner_id', how='left')

    NUP = f"""🙋‍♂️Новые пользователи по партнерам
Партнер - В прошлом месяце - В позапрошлом месяце
{'\n'.join(f'{c} - {lm} - {pr}' for c, lm, pr, in 
            zip(partner_activity['partner_name'].to_list(), 
            partner_activity['active_users_last_month'].to_list(), 
            partner_activity['active_users_two_months_ago'].to_list()))}"""

    plot_partner_activity(
        partner_activity=partner_activity,
        column1='active_users_last_month',
        column2='active_users_two_months_ago',
        xlabel='Партнёры',
        ylabel='Количество активных пользователей',
        title='Активные пользователи из созревших когорт по партнёрам',
        labels=['Прошлый месяц', 'Позапрошлый месяц']
    )


    # Фильтруем активных пользователей за последние два месяца
    active_two_months_ago = user_activity[(user_activity['play_start'].dt.to_period('M') == pd.to_datetime(yesterday_prev_month).to_period('M'))]['user_id'].unique()

    active_last_month = user_activity[(user_activity['play_start'].dt.to_period('M') == pd.to_datetime(yesterday_last_month).to_period('M'))]['user_id'].unique()

    # Находим ушедших пользователей
    churned_users = set(active_two_months_ago) - set(active_last_month)

    if len(active_two_months_ago) > 0:
        churned_users = set(active_two_months_ago) - set(active_last_month)
        churn_rate = len(churned_users) / len(active_two_months_ago) * 100
        symbol_churn_rate = "\U0001F4C8"+"\U00002705" if churn_rate < 50 else "\U0001F4C8 \U0001F53B"
        curn_rate_text = f"🚶Churn Rate за последний месяц:\n{symbol_churn_rate} {churn_rate:.2f}%"
    else:
        curn_rate_text = "💀Нет активных пользователей в позапрошлом месяце\n для расчёта Churn Rate."

    message = f"{message}\n{NUP}\n\n{curn_rate_text}"

    return message

def plot_partner_activity(partner_activity, column1, column2, xlabel, ylabel, title, labels, figsize=(12, 6)):
        plt.figure(figsize=figsize)
        
        # Номера x для столбцов
        x = range(len(partner_activity))
        
        # Построение двух наборов данных
        plt.bar(x, partner_activity[column1], width=0.4, label=labels[0], align='center')
        plt.bar(x, partner_activity[column2], width=0.4, label=labels[1], align='edge')
        
        # Подписи и стили
        plt.xticks(x, partner_activity['partner_name'], rotation=45, ha='right')
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title)
        plt.legend()
        plt.tight_layout()
        plt.savefig('partner_activity.png', format='png')


def simple_line_graph(title, x_data, y_data):
    # Создание DataFrame из данных
    df = pd.DataFrame({
        'x': x_data,
        'y': y_data
    })

    # Построение графика
    plt.figure(figsize=(10, 5))
    plt.plot(df['x'], df['y'], label=y_data.name)

    # Настройка осей и заголовка
    plt.xlabel(x_data.name)
    plt.ylabel(y_data.name)
    plt.title(title)

    plt.xticks(rotation=45)
    plt.legend()

    plt.savefig(f'{title}_line_graph.png')



def combined_graph(title, x_data, bar_y_data, line_y_data):
    # Создание DataFrame для каждого типа данных
    line_df = pd.DataFrame({
        'x': x_data,
        'line_y': line_y_data
    })
    bar_df = pd.DataFrame({
        'x': x_data,
        'bar_y': bar_y_data
    })
    fig, ax = plt.subplots(figsize=(10, 7))

    ax.bar(bar_df['x'], bar_df['bar_y'], color='green', alpha=0.6, label=bar_y_data.name)
    ax.legend(loc='upper left')
    ax.set_xlabel(x_data.name)
    ax.set_ylabel(bar_y_data.name)
    ax.tick_params(axis='x', rotation=45)

    ax2 = ax.twinx()
    ax2.plot(line_df['x'], line_df['line_y'],color='red', label=line_y_data.name)
    ax2.legend(loc='upper right')
    
    plt.title(title)
    plt.savefig(f'{title}_combined_graph.png')

def plot_cac_ltv_ltr(CAC, LTV, filename="cac_ltv_ltr.png"):
    data = {'CAC': CAC, 'LTV': LTV}
    df_ltr = pd.DataFrame(data, index=["Вчера", "Прошлый месяц"])
    
    fig, ax = plt.subplots()
    df_ltr.plot(kind='bar', stacked=True, ax=ax, color=['blue', 'orange'])
    ax.legend(["CAC", "LTV"])
    ax.set_xlabel("Период")
    ax.set_ylabel("Значение показателя")
    ax.tick_params(axis='x', rotation=0)
    plt.title("Вклад CAC и LTV в LTR")
    
    plt.savefig(filename, format='png')

