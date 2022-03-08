#!/usr/bin/env python
# coding: utf-8

# In[8]:


import os
import pandas as pd
import numpy as np

from PIL import Image, ImageSequence
from sklearn.cluster import DBSCAN

def return_statistic(path, start_date, end_date):
    
    stat = pd.DataFrame()
    
    file_names = [name[:-4] for name in os.listdir(path)]
    number_of_files = len(file_names)
    dates = pd.date_range(start=start_date, end=end_date, freq='1D')
    
    stat = pd.DataFrame()
    
    for date in dates:
        stat_this_day = pd.DataFrame({
            'Date':date,
            'Id':file_names
        })
        stat = pd.concat([stat, stat_this_day]).reset_index(drop=True)
    
    Impressions = [int(np.random.normal(1000, 200)) for i in range(len(stat))]
    Clicks = [int(np.random.normal(90, 25)) for i in range(len(stat))]
    Cost = [round(np.random.normal(2000, 400), 2) for i in range(len(stat))]
    Goals = [int(np.random.normal(10, 3)) for i in range(len(stat))]
    
    stat['Impressions'] = Impressions
    stat['Clicks'] = Clicks
    stat['Cost'] = Cost
    stat['Goals'] = Goals
    
    return stat

def return_banners_connector(path):
    
    file_names = [name[:-4] for name in os.listdir(path)]
    number_of_files = len(file_names)
    
    arrays = {}
    shapes = {}
    length = {}
    width = {}
    
    for file_name in file_names:
        im = Image.open('./{}/{}.jpg'.format(path, file_name))
        if type(im).__name__ == 'PngImageFile' or type(im).__name__ == 'JpegImageFile':
            arrays[file_name] = np.asarray(im)
            shapes[file_name] = np.asarray(im).shape[0:2]
            length[file_name] = np.asarray(im).shape[0]
            width[file_name] = np.asarray(im).shape[1]
        elif type(im).__name__ == 'GifImageFile':
            frames = np.array([np.array(frame.copy().convert('RGB').getdata(),dtype=np.uint8).reshape(frame.size[1],frame.size[0],3) for frame in ImageSequence.Iterator(im)])
            arrays[file_name] = frames[0]
            shapes[file_name] = frames[0].shape[0:2]
            length[file_name] = frames[0].shape[0]
            width[file_name] = frames[0].shape[1]
        else:
            print(file_name)
            raise 'Error'
            
    connector = pd.DataFrame({'Id':file_names})
    
    connector['Shape'] = connector['Id'].apply(lambda x:shapes[x])
    connector['Length'] = connector['Id'].apply(lambda x:length[x])
    connector['Width'] = connector['Id'].apply(lambda x:width[x])
    
    min_length = min(connector['Length'])
    min_width = min(connector['Width'])
    
    connector['Coefficient'] = (connector['Length'] / min_length) * (connector['Width'] / min_width)
    
    histograms = {}
    channel_ids = (0, 1, 2)

    for file_path, array in arrays.items():
        temp_list = []
        for channel_id in channel_ids:
            temp_list.extend(list(np.histogram(array[:, :, channel_id], bins=256, range=(0, 256))[0]))
        temp_list_normalized = [j / connector[connector['Id'] == file_path]['Coefficient'].values[0] for j in temp_list]
        histograms[file_path] = temp_list_normalized
    
    df = pd.DataFrame(histograms).transpose()
    clustering = DBSCAN(eps=5500).fit(df.values)
    df['Result'] = clustering.labels_
    df = df.reset_index()[['index', 'Result']].rename(columns={'index':'Id'})
    
    connector = connector.merge(df, on = ['Id'], how='inner')
    
    examples = {}
    n_clusters = connector['Result'].nunique()

    for cluster in range(0, n_clusters):
        data_temp = connector[(connector['Result'] == cluster)].sort_values('Coefficient', ascending=False).reset_index(drop=True)
        examples.update({cluster:data_temp.loc[0, 'Id']})
        
    connector['Example'] = connector['Result'].apply(lambda x:examples[x])
    
#     return connector[['Id', 'Result', 'Example']]
    return connector

def return_dataset(path, start_date, end_date):
    
    stat = return_statistic(path, start_date, end_date)
    connector = return_banners_connector(path)
    
    result = stat.merge(connector, on='Id', how='left')
    
#     campaigns_connector = pd.DataFrame([
#     {'Id':'1', 'Campaign':'Весна', 'Source':'yandex'},
#     {'Id':'2', 'Campaign':'Весна', 'Source':'yandex'},
#     {'Id':'3', 'Campaign':'Весна', 'Source':'yandex'},
#     {'Id':'4', 'Campaign':'Демисезонные_1', 'Source':'yandex'},
#     {'Id':'5', 'Campaign':'Демисезонные_2', 'Source':'google'},
#     {'Id':'6', 'Campaign':'Зима', 'Source':'yandex'},
#     {'Id':'7', 'Campaign':'Зима', 'Source':'yandex'},
#     {'Id':'8', 'Campaign':'Зима', 'Source':'yandex'},
#     {'Id':'9', 'Campaign':'Демисезонные_1', 'Source':'yandex'},
#     {'Id':'10', 'Campaign':'Демисезонные_2', 'Source':'google'},
#     {'Id':'11', 'Campaign':'Лето', 'Source':'google'},
#     {'Id':'12', 'Campaign':'Лето', 'Source':'google'},
#     {'Id':'13', 'Campaign':'Лето', 'Source':'google'},
#     {'Id':'14', 'Campaign':'Демисезонные_1', 'Source':'yandex'},
#     {'Id':'15', 'Campaign':'Демисезонные_2', 'Source':'google'},
#     {'Id':'16', 'Campaign':'Осень', 'Source':'google'},
#     {'Id':'17', 'Campaign':'Осень', 'Source':'google'},
#     {'Id':'18', 'Campaign':'Осень', 'Source':'google'},
#     {'Id':'19', 'Campaign':'Демисезонные_1', 'Source':'yandex'},
#     {'Id':'20', 'Campaign':'Демисезонные_2', 'Source':'google'}
#     ])
    
#     result = result.merge(campaigns_connector, on='Id', how='left')
    
    return result

if __name__ == '__main__':
    data = return_dataset('pictures', '2022-02-01', '2022-02-05')

