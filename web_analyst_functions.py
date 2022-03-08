#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import warnings
import requests
from datetime import date, timedelta, datetime
warnings.filterwarnings("ignore")

# Яндекс Директ

def yandex_direct(clientLoginlist:list, start_date:str, end_date:str, token, fields=['Date', 'CampaignId', 'Impressions', 'Clicks', 'Cost'], account_name=False, rename_columns = {}):
    
    import sys
    import random
    import json
    import requests
    from time import sleep
    
    if sys.version_info < (3,):
        def u(x):
            try:
                return x.encode("utf8")
            except UnicodeDecodeError:
                return x
    else:
        def u(x):
            if type(x) == type(b''):
                return x.decode('utf8')
            else:
                return x
            
    ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'
    directdict = {}
    
    for login in clientLoginlist:
        headers = {
                   "Authorization": "Bearer " + token,
                   "Client-Login": login,
                   "Accept-Language": "ru",
                   "processingMode": "auto"
                }

        body = {
                            "params": {
                                "SelectionCriteria": {
                                    "DateFrom": start_date,
                                    "DateTo": end_date

                                },
                                "Goals": ["1"],
                                "AttributionModels": ["LSC"],
                                "FieldNames": fields,
                                "ReportName": u("D5%sfdfGGWstD%sddfEG" % (random.randint(0, 1000),random.randint(0, 1000))),
                                "ReportType": "CUSTOM_REPORT",
                                "DateRangeType": "CUSTOM_DATE",
                                "Format": "TSV",
                                "IncludeVAT": "NO",
                                "IncludeDiscount": "NO"
                            }
                        }
        body = json.dumps(body, indent=4)

        while True:
            try:
                req = requests.post(ReportsURL, body, headers=headers)
                req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
                if req.status_code == 400:
                    print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код запроса: {}".format(u(body)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                elif req.status_code == 200:
                    format(u(req.text))
                    break
                elif req.status_code == 201:
                    print("Отчет успешно поставлен в очередь в режиме офлайн")
                    retryIn = int(req.headers.get("retryIn", 60))
                    print("Повторная отправка запроса через {} секунд".format(retryIn))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    sleep(retryIn)
                elif req.status_code == 202:
                    print("Отчет формируется в режиме офлайн")
                    retryIn = int(req.headers.get("retryIn", 60))
                    print("Повторная отправка запроса через {} секунд".format(retryIn))
                    print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                    sleep(retryIn)
                elif req.status_code == 500:
                    print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                elif req.status_code == 502:
                    print("Время формирования отчета превысило серверное ограничение.")
                    print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                    print("JSON-код запроса: {}".format(body))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                else:
                    print("Произошла непредвиденная ошибка")
                    print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код запроса: {}".format(body))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break

            # Обработка ошибки, если не удалось соединиться с сервером API Директа
            except ConnectionError:
                # В данном случае мы рекомендуем повторить запрос позднее
                print("Произошла ошибка соединения с сервером API")
                # Принудительный выход из цикла
                break

            # Если возникла какая-либо другая ошибка
            except:
                # В данном случае мы рекомендуем проанализировать действия приложения
                print("Произошла непредвиденная ошибка")
                # Принудительный выход из цикла
                break

        file = open("cashez.csv", "w")
        file.write(req.text)
        file.close()
        direct = pd.read_csv("cashez.csv", header=1, sep='	', index_col=0, engine='python')

        direct.Cost = direct.Cost/1000000
        direct.Cost = direct.Cost*1.2


        direct = direct.iloc[:-1]
        direct.sort_values('Date', inplace = True)
        direct.reset_index(inplace = True)
        if account_name:
            direct['Login'] = login
        if 'CampaignId' in fields:
            direct['CampaignId'] = direct['CampaignId'].astype('int64')
            direct['CampaignId'] = direct['CampaignId'].astype('str')
        directdict[login] = direct

    direct = pd.concat(directdict)
    direct.reset_index(inplace=True, drop=True)
    
    return direct.rename(columns=rename_columns)

# Гугл Эдс

def google_adwords(adwords_ids:list, start_date:str, end_date:str, cridentials='googleads.yaml', fields=['Date', 'CampaignId', 'Impressions', 'Clicks', 'Cost'], report_type = 'CAMPAIGN_PERFORMANCE_REPORT', account_id=False, rename_columns = {'Day':'Date', 'Campaign ID':'CampaignId', 'Campaign':'CampaignName', 'Keyword / Placement':'Criteria'}):
    
    import io
    from googleads import adwords
    
    adwdict = {}
    
    for adwords_id in adwords_ids:

        output = io.StringIO()

        adwords_client = adwords.AdWordsClient.LoadFromStorage(cridentials)

        adwords_client.SetClientCustomerId(adwords_id)

        report_downloader = adwords_client.GetReportDownloader(version='v201809') 

        report_query = ('''
            select {}
            from {}
            during {},{}'''.format(','.join(fields), report_type, start_date.replace('-', ''), end_date.replace('-', '')))

        report_downloader.DownloadReportWithAwql(
            report_query, 
            'CSV',
            output,
            client_customer_id=adwords_id, 
            skip_report_header=True, 
            skip_column_header=False,
            skip_report_summary=True, 
            include_zero_impressions=False)

        output.seek(0)

        adw = pd.read_csv(output)
        
        if account_id:
            adw['Login'] = adwords_id
        if 'CampaignId' in fields:
            adw['Campaign ID'] = adw['Campaign ID'].astype('int64')
            adw['Campaign ID'] = adw['Campaign ID'].astype('str')

        adwdict[adwords_id]=adw
        
    adw = pd.concat(adwdict)
    adw.reset_index(inplace=True, drop=True)
    
    return adw.rename(columns=rename_columns)

# Яндекс Метрика

def yandex_metrika(token, metrika_id:str, start_date:str, end_date:str, metrics:list, dimensions=['ym:s:date', 'ym:s:lastsignUTMSource', 'ym:s:lastsignUTMMedium', 'ym:s:lastsignUTMCampaign'], rename_columns={}):
    
    from requests_html import HTMLSession
    from flatten_json import flatten

    session = HTMLSession()
    metrika_headers = {
    'GET': '/management/v1/counters HTTP/1.1',
    'Host': 'api-metrika.yandex.net',
    'Authorization': token,
    'Content-Type': 'application/x-yametrika+json'
    }
    
    metrika_url = 'https://api-metrika.yandex.net/stat/v1/data'
    
    payload = {
    'ids': metrika_id,
    'metrics': ','.join(metrics),
    'accuracy': 'full',
    'date1': start_date,
    'date2': end_date,
    'dimensions': ','.join(dimensions),
    'include_undefined':True,
    'lang':'ru',
    'limit':100000,
    'pretty':'0',
    'proposed_accuracy': False
    }
    
    json_content_response = session.get(metrika_url, params = payload, headers=metrika_headers).json()
    dic_flattened = [flatten(d) for d in json_content_response['data']]
    metrica = pd.DataFrame(dic_flattened)
    
    return metrica.rename(rename_columns)

# Гугл Аналитикс

def google_analytics(view_id:str, start_date:str, end_date:str, metrics:list, dimensions=['ga:date', 'ga:source', 'ga:medium', 'ga:campaign'], cridentials='client_secret.json', filtersExpression='', orderBys=[], segments=[], group_by=[], rename_columns={'ga:date':'Date', 'ga:source':'Source', 'ga:medium':'Medium', 'ga:campaign':'Campaign'}):
    
    from gaData import return_ga_data, initialize_analyticsreporting
    
    CLIENT_SECRETS_PATH = cridentials
    analytics_initialize = initialize_analyticsreporting(CLIENT_SECRETS_PATH)
    
    analytics = return_ga_data(analytics_initialize,
                      start_date= start_date,
                      end_date=end_date,
                      view_id=view_id,
                      metrics=[{'expression':metrica} for metrica in metrics],
                      dimensions=[{'name':dimension} for dimension in dimensions],
                      filtersExpression = filtersExpression,
                      orderBys = orderBys,
                      segments = segments,
                      group_by = group_by
                     )
    return analytics.rename(columns=rename_columns)

# Фэйсбук

def facebook(account_id:str, start_date:str, end_date:str, access_token:str, fields=['campaign_id', 'impressions', 'clicks', 'spend'], level='campaign', breakdowns=[], rename_columns={'campaign_id':'CampaignId', 'date_start':'Date', 'impressions':'Impressions', 'clicks':'Clicks', 'spend':'Cost'}):
    
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    
    params = {
    'time_range': {'since': start_date,'until':end_date},
    'time_increment':1,
    'level': level,
    'breakdowns':breakdowns
    } 
    
    FacebookAdsApi.init(access_token=access_token)
    
    facebook = AdAccount(account_id).get_insights(
        fields=fields,
        params=params,
    )
    
    facebook = pd.DataFrame(facebook)
    facebook.drop('date_stop', axis=1, inplace=True)
    
    facebook = facebook[['date_start'] + fields]
    
    return facebook.rename(columns=rename_columns)

# Вконтакте

def vkontakte(start_date:str, end_date:str, token:str, id_rk:int, client_id:int):
    version = 5.103
    
    def return_campaigns(token, id_rk, client_id):
        r = requests.get('https://api.vk.com/method/ads.getCampaigns', params={
            'access_token': token,
            'v': version,
            'account_id': id_rk,
            'client_id' : client_id
            })

        campaigns = r.json()['response']
        campaigns = pd.DataFrame(campaigns)
        campaigns = campaigns[['id', 'name']]

        campaign_ids_list = list(campaigns['id'])

        return campaigns.rename(columns={'id':'CampaignId', 'name':'CampaignName'}), campaign_ids_list
    
    connector, campaign_ids_list = return_campaigns(token, id_rk, client_id)
    campaign_ids_str = ''

    for campaign_id in campaign_ids_list:
        campaign_ids_str += str(campaign_id) + ','
    campaign_ids_str = campaign_ids_str[:-1]
    
    r = requests.get('https://api.vk.com/method/ads.getStatistics', params={
    'access_token': token,
    'v': version,
    'account_id': id_rk,
    'ids_type': 'campaign',
    'ids': campaign_ids_str,
    'period': 'day',
    'date_from': start_date,
    'date_to': end_date
    })

    dataset = pd.DataFrame(r.json()['response'])
    data_vk = pd.DataFrame()

    for num, campaign in enumerate(campaign_ids_list):
        data_campaign = dataset[dataset.id == campaign]
        data_stats = pd.DataFrame(data_campaign['stats'][num])
        data_stats['CampaignId'] = campaign
        data_vk = data_vk.append(data_stats)

    data_vk.fillna(0, inplace=True)
    data_vk = data_vk.merge(connector, on='CampaignId', how='left')
    data_vk.rename(columns={
        'day':'Date',
        'impressions':'Impressions',
        'clicks':'Clicks',
        'spent':'Cost'
    }, inplace=True)
    data_vk.drop('reach', axis=1, inplace=True)
    
    return data_vk[['Date', 'CampaignId', 'CampaignName', 'Impressions', 'Clicks', 'Cost']]

# ГБКУ

def google_big_query(data, table_name, rewrite:bool, rewrite_condition:str, column_types:dict, disposition='APPEND', cridentials='gbq_cred.json'):
    
    import os
    from google.cloud import bigquery
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cridentials
    client = bigquery.Client()
    
    if rewrite:
        dml_query = """
        DELETE {} 
        WHERE """.format(table_name) + rewrite_condition
        dml_query_job = client.query(dml_query)
    
    gbq_types = {
    'date':bigquery.enums.SqlTypeNames.DATE,
    'datetime':bigquery.enums.SqlTypeNames.DATETIME,
    'timestamp':bigquery.enums.SqlTypeNames.TIMESTAMP,
    'time':bigquery.enums.SqlTypeNames.TIME,
    'str':bigquery.enums.SqlTypeNames.STRING,
    'bytes':bigquery.enums.SqlTypeNames.BYTES,
    'int':bigquery.enums.SqlTypeNames.INTEGER,
    'float':bigquery.enums.SqlTypeNames.FLOAT,
    'numeric':bigquery.enums.SqlTypeNames.NUMERIC,
    'bool':bigquery.enums.SqlTypeNames.BOOLEAN,
    'geography':bigquery.enums.SqlTypeNames.GEOGRAPHY,
    'record':bigquery.enums.SqlTypeNames.RECORD
    }
    
    schema = [bigquery.SchemaField(col_name, gbq_types[column_types[col_name]]) for col_name in column_types.keys()]
    
    job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition="WRITE_{}".format(disposition)
    )
    
    job = client.load_table_from_dataframe(data, table_name, job_config=job_config)
    job.result()
    
    table = client.get_table(table_name)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, 
            len(table.schema), 
            table_name
        )
    )
    
    return None


# In[ ]:




