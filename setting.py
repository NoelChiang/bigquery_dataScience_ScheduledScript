# Defined mail receivers
mail_notice_observers = ['noel_chiang@friday.tw']
# Defined chart folder
chart_resource_folder = 'charts/'
# Defined chart output path
chart_path_daily_session =  chart_resource_folder+'daily_session_chart.png'
chart_path_home_marketing = chart_resource_folder+'home_marketing_chart.png'
chart_path_operations =     chart_resource_folder+'operations_chart.png'
chart_path_daily_purchase = chart_resource_folder+'daily_purchase_chart.png'
chart_path_banner_summary = chart_resource_folder+'banner_summary.png'

def get_mail_notify_attachments():
    return [chart_path_daily_purchase, chart_path_banner_summary]

