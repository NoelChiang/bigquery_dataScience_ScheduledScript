import math
from datetime import datetime, timedelta

# -- Import my bigquery package
from MyModule import BigQueryTool

# -- Import setting file for global parameters
import setting

# -- Import graph library and set
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
sns.set()

# Set font to support chinese
mpl.rcParams['font.family']='Heiti TC'
mpl.rcParams['axes.unicode_minus']=False # in case minus sign is shown as box    

def makeDateInterval(start: datetime = None, end: datetime = None):
    # Return tuples with query start date and finish date
    # Format : YYYYMMdd
    # Meet bigquery daily dataset naming (ex. events_20200606)
    endTime = end if end is not None else datetime.now()
    endYear = str(endTime.year)
    endMonth = "0"+str(endTime.month) if len(str(endTime.month)) < 2 else str(endTime.month)
    endDay = "0"+str(endTime.day) if len(str(endTime.day)) < 2 else str(endTime.day)
    endDate = endYear + endMonth + endDay
    startTime = start if start is not None else endTime - timedelta(days=30)
    startYear = str(startTime.year)
    startMonth = "0"+str(startTime.month) if len(str(startTime.month)) < 2 else str(startTime.month)
    startDay = "0"+str(startTime.day) if len(str(startTime.day)) < 2 else str(startTime.day)
    startDate = startYear + startMonth + startDay
    return (startDate, endDate)

def makeGraphTitleDateString(start: str, end: str):
    # Return string to present date interval
    # Format: YYYY/MM/dd~YYYY/MM/dd
    # Input format: YYYYMMdd
    return start[:4]+"/"+start[4:6]+"/"+start[6:]+"~"+end[:4]+"/"+end[4:6]+"/"+end[6:]

def drawDailySessionChart(start: datetime = None, end: datetime = None):
    dateInterval = makeDateInterval(start=start, end=end)
    queryStr = '''
                SELECT user_pseudo_id AS id, event_timestamp as ts
                FROM `analytics_178973991.events_*`
                WHERE event_name='session_start'
                AND _TABLE_SUFFIX BETWEEN '{0}'
                AND '{1}'
                '''.format(dateInterval[0], dateInterval[1])
    query = BigQueryTool.SqlCommander().send(queryStr)

    user_sessions = query.to_dataframe()
    # Transfer timestamp to datetime and ordering by ascending
    user_sessions.sort_values('ts', inplace=True)
    user_sessions['launch_date'] = [datetime.fromtimestamp(time/1000000).date()
                            for time in user_sessions['ts']]
    # DataFrame: Count of all session by date
    # Remove first 2 days' data (un-completed)
    user_sessions_all = user_sessions.groupby('launch_date').count()
    user_sessions_all.drop(user_sessions_all.index[[0,1]], inplace=True)
    # DataFrame: Count of user's first launch by date
    # Remove first 2 days' data (un-completed)
    user_sessions_first = user_sessions.drop_duplicates(subset='id')
    user_sessions_first = user_sessions_first.groupby('launch_date').count()
    # user_sessions_first.drop(user_sessions_first.index[[0,1]], inplace=True)
    # Generate table of first launch and all session
    user_sessions_table = pd.DataFrame({'new': user_sessions_first['id'], 
                                        'all': user_sessions_all['id']}, 
                                        index=user_sessions_all.index)
    fig, ax = plt.subplots(figsize=(16,6))
    user_sessions_table.plot(
        y=['new','all'], 
        kind='bar', 
        ax=ax, 
        label=['First launch', 'All Sessions'], 
        rot=0, 
        fontsize=14)
    ax.set_xlabel('Date', fontsize=14)
    ax.set_ylabel('Counts of Session', fontsize=14)
    ax.set_xticklabels(labels=[str(date.month)+'/'+str(date.day) 
                                for date in user_sessions_table.index])
    begin_time = user_sessions['ts'].min() / 1000000
    finish_time = user_sessions['ts'].max() / 1000000
    print("begin:", begin_time, ", finish:", finish_time)
    ax.set_title('friDay購物 App Daily Sessions Counts\n({0})'.format(makeGraphTitleDateString(dateInterval[0], dateInterval[1])), fontsize=20, fontweight='bold')
    ax.legend(frameon=False, fontsize=14, ncol=2)
    fig.savefig(setting.chart_path_daily_session, bbox_inches='tight')

def drawHomeMarketingSummaryChart(start: datetime = None, end: datetime = None):
    dateInterval = makeDateInterval(start=start, end=end)
    queryStr = '''
        SELECT event_name AS event, platform, event_timestamp As time,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE  key='content') AS content
        FROM `analytics_178973991.events_*`
        WHERE (event_name='mt_home_promo_click' 
        OR event_name='mt_home_banner_click' 
        OR event_name='mt_home_product_click')
        AND _TABLE_SUFFIX BETWEEN '{0}'
        AND '{1}'
    '''.format(dateInterval[0], dateInterval[1])
    query_home_market_event = BigQueryTool.SqlCommander().send(queryStr)
    home_market_events = query_home_market_event.to_dataframe()
    queryStr = '''
        SELECT COUNT(event_name) AS count, platform,
        FROM `analytics_178973991.events_*`
        WHERE event_name='session_start'
        AND _TABLE_SUFFIX BETWEEN '{0}'
        AND '{1}'
        GROUP BY platform
    '''.format(dateInterval[0], dateInterval[1])
    query_session_count = BigQueryTool.SqlCommander().send(queryStr)
    session_count = query_session_count.to_dataframe()
    # Convert DataFrame to Series
    session_count = session_count.set_index('platform').iloc[:, 0]
    # Func to set contents to corresponded category
    def event_category(event, content):
        if event == 'mt_home_banner_click':
            if 'f幣' in content:
                return '領取F幣'
            elif '折價券' in content:
                return '領取折價券'
            elif '生活提案' in content:
                return '生活提案點擊'
            else:
                return '廣告橫幅點擊'
        elif event == 'mt_home_promo_click':
            if '喜好' in content:
                return '標籤點擊\n(個人/關企)'
            else:
                return '好康/大好康'
        elif event == 'mt_home_product_click':
            if '好康' in content:
                return '好康/大好康'
            else:
                return '商品點擊\n(個人興趣/關企推薦)'
        else:
            return ''
        
    home_market_events.dropna(inplace=True)
    home_market_events['event_category'] = home_market_events.apply(
        lambda x: event_category(x['event'], x['content']), axis=1)
    home_market_table = home_market_events.pivot_table('content', 
                                                        index=['event_category'], 
                                                        columns=['platform'], 
                                                        aggfunc='count')
    session_count.name = 'Session Start'
    # Append session_count as last row
    home_market_table = home_market_table.append(session_count)
    def set_bar_text(axes):
        total = home_market_table.sum(axis=1).loc['Session Start']
        for i in range(len(home_market_table.index) - 1):
            percentage = home_market_table.sum(axis=1).iloc[i] / total * 100
            percentage =' '+str(round(percentage, 2))+'%'
            if i == len(home_market_table.index) - 2:
                percentage += ' --------- (事件次數 / 總Session數)'
            ax.text(home_market_table.sum(axis=1).iloc[i], 
                    i - 0.05, 
                    percentage, 
                    fontsize=15, 
                    fontweight='bold')
    fig, ax = plt.subplots(figsize=(15,10))
    with sns.color_palette('Accent', 8):
        home_market_table.plot(ax=ax, kind='barh', stacked=True, width=0.4, fontsize=18)
    ax.legend(frameon=False, fontsize= 20, ncol=2)
    ax.set_xlabel('事件次數', fontsize=18)
    # latest_time = home_market_events['time'].max()
    # latest_date = datetime.fromtimestamp(latest_time/1000000)
    ax.set_title('friDay購物 APP首頁行銷行為統計\n({0})'.format(makeGraphTitleDateString(dateInterval[0],dateInterval[1])), fontsize=20)
    ax.set_ylabel('')
    set_bar_text(ax)
    fig.savefig(setting.chart_path_home_marketing, bbox_inches='tight')

def drawOperationSummaryChart(start: datetime = None, end: datetime = None):
    dateInterval = makeDateInterval(start=start, end=end)
    queryStr = '''
        SELECT platform,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key='content') AS content
        FROM `analytics_178973991.events_*`,
        UNNEST(event_params) AS params
        WHERE event_name='UIOperation' 
        AND params.key='content' 
        AND params.value.string_value LIKE '%search_use%'
        AND _TABLE_SUFFIX BETWEEN '{0}'
        AND '{1}'
    '''.format(dateInterval[0], dateInterval[1])
    print(queryStr)
    query_search_events = BigQueryTool.SqlCommander().send(queryStr)
    search_events = query_search_events.to_dataframe()
    def search_keyword_source(content):
        if 'history' in content:
            return '歷史記錄'
        elif 'popular' in content:
            return '熱門搜尋'
        elif 'ATT' in content:
            return '語音輸入'
        else:
            return 'Unknown'
    search_events.dropna(inplace=True)
    search_events['keyword_source'] = search_events.apply(
        lambda x: search_keyword_source(x['content']), axis=1)

    queryStr = '''
        SELECT event_date AS date, event_name AS event, platform
        FROM `analytics_178973991.events_*`
        WHERE (event_name='ecommerce_purchase' 
        OR event_name='view_item' 
        OR event_name='search' 
        OR event_name='checkout_progress' 
        OR event_name='begin_check' 
        OR event_name='add_to_cart'
        OR event_name='session_start')
        AND _TABLE_SUFFIX BETWEEN '{0}'
        AND '{1}'
    '''.format(dateInterval[0], dateInterval[1])
    query = BigQueryTool.SqlCommander().send(queryStr)
    operations = query.to_dataframe()
    operations_table = operations.pivot_table('date', 
                                            index='event', 
                                            columns='platform', 
                                            aggfunc='count')
    search_events_table = search_events.pivot_table('content', 
                                                    index=['keyword_source'],
                                                    columns=['platform'],
                                                    aggfunc='count')
    operation_search_series = operations_table.loc['search'] - search_events_table.sum()
    operation_search_series.name = '鍵盤輸入'
    search_events_table = search_events_table.append(operation_search_series)
    fig = plt.figure(figsize=(15,15))
    grid = fig.add_gridspec(2, 2, wspace=0.03, hspace=0.5)
    ax0 = plt.subplot(grid[0,0])
    ax1 = plt.subplot(grid[0,1], sharey=ax0)
    ax2 = plt.subplot(grid[1,0])
    ax3 = plt.subplot(grid[1,1])

    operations_table_for_graph = operations_table.drop(index=['session_start'])
    operations_table_for_graph /= operations_table.loc['session_start']

    # Remove un-reasonable data ['view_item', 'checkout_progress']
    operations_table_for_graph.drop(['view_item', 'checkout_progress'], inplace=True)

    operations_table_for_graph['ANDROID'].plot(ax=ax0, kind='barh',width=0.25, color='g')
    operations_table_for_graph['IOS'].plot(ax=ax1, kind='barh', width=0.25, color='y')

    def mapping_index(index):
        if 'add_to_cart' in index: return '加入購物車'
        if 'checkout_progress' in index: return '購物車Step2'
        if 'ecommerce_purchase' in index: return '完成結帳'
        if 'search' in index: return '搜尋功能'
        if 'view_item' in index: return '檢視商品'

    def set_bar_text(axes, column):
        index = 0
        for p in axes.patches:
            axes.annotate(round(operations_table_for_graph[column].iloc[index], 2), 
                        (p.get_width(),  p.get_y()), 
                        fontsize=15)
            index += 1

    ax0.set_yticklabels(labels=[mapping_index(index) 
                            for index in operations_table_for_graph.index],
                        fontsize=15, 
                        fontweight='bold')
    ax0.set_xticklabels(labels=[])
    ax0.set_ylabel('')
    ax0.set_xlim([0,operations_table_for_graph['ANDROID'].max() * 1.2])
    ax0.set_title('每次使用期間平均發生次數 - ANDROID', fontsize=16, fontweight='bold')
    ax0.set_xlabel('Event ount / Session count', fontsize=15)
    ax1.set_xticklabels(labels=[])
    ax1.set_xlim([0,operations_table_for_graph['IOS'].max() * 1.2])
    ax1.set_title('每次使用期間平均發生次數 - IOS', fontsize=16, fontweight='bold')
    ax1.set_xlabel('Event ount / Session count', fontsize=15)
    set_bar_text(ax0, 'ANDROID')  
    set_bar_text(ax1, 'IOS')

    text_props = {'fontsize': 15, 'fontweight': 'bold'}
    explode_values = []
    for i in range(len(search_events_table.index)):
        explode_values.append(i * 0.1)
    with sns.color_palette('Pastel2'):
        search_events_table['ANDROID'].sort_values(ascending=False).plot(ax=ax2, 
                                                                        kind='pie', 
                                                                        autopct='%1.1f%%', 
                                                                        startangle=90, 
                                                                        explode=explode_values, 
                                                                        textprops=text_props)
        search_events_table['IOS'].sort_values(ascending=False).plot(ax=ax3, 
                                                                    kind='pie', 
                                                                    autopct='%1.1f%%', 
                                                                    startangle=90, 
                                                                    explode=explode_values, 
                                                                    textprops=text_props)
    ax2.set_title('ANDROID\n搜尋方式統計', loc='left', fontsize=18, fontweight='bold')
    ax2.set_ylabel('')
    ax3.set_title('IOS\n搜尋方式統計', loc='left', fontsize=18, fontweight='bold')
    ax3.set_ylabel('')
    fig.suptitle('friDay購物 App 操作事件統計\n({0})'.format(makeGraphTitleDateString(dateInterval[0], dateInterval[1])), fontsize=24, fontweight='heavy')
    fig.savefig(setting.chart_path_operations, bbox_inches='tight')

def drawDailyPurchaseChart(start: datetime = None, end: datetime = None):
    dateInterval = makeDateInterval(start=start, end=end)
    queryStr = '''
        SELECT platform, event_timestamp AS time,
        (SELECT value.double_value FROM UNNEST(event_params) WHERE key='value') 
        AS double_value,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key='value') 
        AS int_value,
        (SELECT value.double_value FROM UNNEST(event_params) WHERE key='price') 
        AS double_price,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key='price') 
        AS int_price,
        FROM `analytics_178973991.events_*`
        WHERE event_name='ecommerce_purchase'
        AND _TABLE_SUFFIX BETWEEN '{0}'
        AND '{1}' 
    '''.format(dateInterval[0], dateInterval[1])
    query_purchase = BigQueryTool.SqlCommander().send(queryStr)
    purchases = query_purchase.to_dataframe()
    def get_final_value(double_value, int_value, double_price, int_price):
        # Prevent from 0 value
        # Retrive data priority: 
        # value(double) -> value(int) -> price(double) -> price(int)
        if not math.isnan(double_value) and double_value != 0:
            return double_value
        if not math.isnan(int_value) and int_value != 0:
            return int_value
        if not math.isnan(double_price) and double_price != 0:
            return double_price
        if not math.isnan(int_price) and int_price != 0:
            return int_price
        return float('nan')
    purchases['revenue'] = purchases.apply(
        lambda x: get_final_value(x['double_value'], 
                                x['int_value'], 
                                x['double_price'], 
                                x['int_price']), 
        axis=1)
    purchases['date'] = [datetime.fromtimestamp(time/1000000).date()
                                        for time in purchases['time']]
    queryStr = '''
        SELECT event_name AS event, event_timestamp AS time
        FROM `analytics_178973991.events_*`
        WHERE event_name='session_start' OR event_name='ecommerce_purchase'
        AND _TABLE_SUFFIX BETWEEN '{0}'
        AND '{1}'
    '''.format(dateInterval[0], dateInterval[1])
    query = BigQueryTool.SqlCommander().send(queryStr)
    sessions = query.to_dataframe()
    sessions['date'] = [datetime.fromtimestamp(time/1000000).date() 
                        for time in sessions['time']]
    conversion_table = sessions.pivot_table('time', 
                                            index='date', 
                                            columns='event',
                                            aggfunc='count').dropna()
    def get_rate(purchase_count, session_count):
        return purchase_count / session_count
    conversion_table['rate'] = conversion_table.apply(
        lambda x: get_rate(x['ecommerce_purchase'], x['session_start']), axis=1)
    final_revenue_table = conversion_table.join(purchases.pivot_table('revenue', 
                                                                    index='date', 
                                                                    aggfunc='sum')/1000000)
    final_revenue_table['date'] = final_revenue_table.index
    final_revenue_table.index = [str(d.month)+'/'+str(d.day) 
                                for d in final_revenue_table['date']]
    fig, ax = plt.subplots(figsize=(15,10))
    ax3 = ax.twinx()
    ax2 = ax.twinx()
    ax2.spines['right'].set_position(('axes', 1.11))
    with sns.axes_style('white'):
        final_revenue_table['revenue'].plot(ax=ax, 
                                            kind='bar', 
                                            color='g', 
                                            label='銷售額', 
                                            rot=0, 
                                            fontsize=16, 
                                            width=-0.25, 
                                            align='edge')
        final_revenue_table['ecommerce_purchase'].plot(ax=ax3, 
                                                    color='c', 
                                                    width=0.25, 
                                                    align='edge',
                                                    kind='bar', 
                                                    label='訂單數', 
                                                    fontsize=16)
        final_revenue_table['rate'].plot(ax=ax2, 
                                        color='brown', 
                                        label='轉換率', 
                                        fontsize=16)
        
    ax.set_ylim([0, final_revenue_table['revenue'].max() * 1.2])
    ax.legend(loc='upper left', frameon=False, fontsize=18)
    ax.set_ylabel('銷售額(百萬元)', fontsize=20, fontweight='bold', labelpad=30)
    ax.set_title('friDay購物 APP每日銷售統計\n({0})'.format(makeGraphTitleDateString(dateInterval[0], dateInterval[1])), fontsize=24, fontweight='bold')
    ax.grid(False)
    ax2.set_ylim([0, final_revenue_table['rate'].max() * 1.2])
    ax2.legend(loc='upper right', frameon=False, fontsize=18)
    ax2.set_ylabel('轉換率(Checkout count / Session count)', 
                fontsize=20, 
                fontweight='bold', 
                labelpad=20)
    ax2.yaxis.set_major_formatter(mpl.ticker.PercentFormatter(xmax=1.0, decimals=2))
    ax3.set_ylim([0, final_revenue_table['ecommerce_purchase'].max() * 1.2])
    ax3.legend(loc='upper center', frameon=False, fontsize=18)
    ax3.set_ylabel('訂單數', fontsize=20, fontweight='bold', labelpad=20)
    ax3.grid(False)
    fig.savefig(setting.chart_path_daily_purchase, bbox_inches='tight')

def drawHomeBannerClickChart(start: datetime = None, end: datetime = None):
    dateInterval = makeDateInterval(start=start, end=end)
    queryStr = '''
        SELECT event_timestamp AS time, platform, 
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key='content') AS content
        FROM `analytics_178973991.events_*`, UNNEST(event_params) AS params
        WHERE event_name='mt_home_banner_click' 
        AND params.key='content' 
        AND params.value.string_value LIKE '%anner@%'
        AND _TABLE_SUFFIX BETWEEN '{0}'
        AND '{1}'
    '''.format(dateInterval[0], dateInterval[1])
    query =BigQueryTool.SqlCommander().send(queryStr)
    banners = query.to_dataframe()
    def check_store(content):
        if 'city' in content:
            return "city'super"
        if 'friDay' in content or '全站商品' in content:
            return "friDay"
        if 'SOGO' in content: 
            return "SOGO"
        if '遠東百貨' in content:
            return '遠東百貨'
        if '愛買線上購物' in content:
            return '愛買'
        return 'Others'
    def extract_topic(content):
        return content[content.find('@{')+2:-1]
    banners['store'] = banners['content'].apply(lambda x: check_store(x))
    banners['topic'] = banners['content'].apply(lambda x: extract_topic(x))
    index = banners[banners['topic']=='null'].index
    banners.drop(index, inplace=True)
    banners_table = banners.pivot_table('content', 
                                    index=['topic', 'store'], 
                                    aggfunc='count')
    banners_table = banners_table.reset_index().set_index('topic')
    banners_table.sort_values('content', inplace=True)
    colors = {'friDay': '#F34F59', 
            'SOGO': 'orange', 
            '遠東百貨': '#004EA2', 
            '愛買': '#D50039', 
            "city'super": '#27781E'}
    color_set = banners_table['store'].apply(lambda x: colors[x])
    fig, ax = plt.subplots(figsize=(15,15))
    banners_table['content'].tail(30).plot(ax=ax, 
                                        kind='barh', 
                                        color=color_set.tail(30), 
                                        fontsize=18,
                                        width = 0.35)
    legend_labels = list(colors.keys())
    legend_handles = [plt.Rectangle((0,0), 1, 1, color=colors[label]) 
                                for label in legend_labels]
    ax.legend(labels=legend_labels, 
            handles=legend_handles, 
            loc='lower right', 
            fontsize=20)
    ax.set_ylabel('')
    ax.set_xlabel('點擊次數', fontsize=18)
    ax.set_title('friDay購物 APP首頁廣告橫幅點擊統計 Top30\n({0})'.format(makeGraphTitleDateString(dateInterval[0], dateInterval[1])), 
                fontsize=24, 
                fontweight='heavy')
    fig.savefig(setting.chart_path_banner_summary, bbox_inches='tight')
