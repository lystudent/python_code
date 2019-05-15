# -*- coding: utf-8 -*-
# @Time    : 2019-04-23 11:31
# @Author  : liuyanming
# @Email   : 1420743191@qq.com
# @File    : wmo_pool.py
# @Software: PyCharm

# 导入必要模块
import pandas as pd
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup
import urllib
import json
import pandas as pd
import subprocess
import re
import time
from concurrent.futures import ProcessPoolExecutor
import argparse

engine = create_engine('mysql+pymysql://root:123456@47.94.211.104:3306/wmo')


def get_valuedata(station_id, station_name, country, year, att_key="Cumul%20precip"):
    proxies = {
        "http": "http://119.102.185.145:9999",
        "http": "http://119.102.184.200:9999",
        "http": "http://119.102.129.161:9999",
        "http": "http://119.102.130.193:9999",
        "http": "http://119.102.130.37:9999",
        "http": "http://110.52.235.145:9999",
        "http": "http://119.102.130.87:9999",
    }
    #     station_id = 50468
    #     station_name = "Aihui"
    country = country
    frequency = "Dekadal"
    attribute = {"Average%20temp": "Average%20Temperature",
                 "Cumul%20precip": "Cumulative%20Precipitation",
                 "Maximum%20temp": "Maximum%20Temperature",
                 "Minimum%20temp": "Minimum%20Temperature",
                 "Precipitation": "Precipitation",
                 "Percent%20soil%20moist": "Percent%20Soil%20Moisture",
                 "Subsurface%20moisture": "Subsurface%20Soil%20Moisture",
                 "Surface%20moisture": "Surface%20Soil%20Moisture"
                 }
    #     att_key =  "Maximum%20temp"
    att_value = attribute.get(att_key)

    print(f"************************开始爬取站点{station_id}的数据****************************")
    key_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        'Content-Type': 'text/x-gwt-rpc; charset=UTF-8',
        'X-GWT-Module-Base': 'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/',
        'X-GWT-Permutation': 'AEDAC353BD2C774915FB7B85744D4FFF',
        'Referer': f'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/?&feats={station_id}&type=S&name={station_name},%20{country}&title=WMO%20{att_value}%20for%20{station_name},%20{country}%20(WMO%20Station%20{station_name})&freq={frequency}&att={att_value}&years={year}&style=line&start={year}0101&end={year}1231',
        'Connection': 'keep-alive'
    }
    key_data = '7|0|5|https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/|386CE0E7206C8C15FC85203A478BDF82|gov.usda.fas.ipad.cadre.gwt.client.CadreQuickGraphsService|getKey|java.lang.String/2004016611|1|2|3|4|1|5|0|'
    key_response = requests.post(
        'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/graphservice', verify=False,
        headers=key_headers, data=key_data, proxies=proxies)
    key = eval(key_response.text.split("//OK")[1])[1][0]
    print(key)

    files_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        'Content-Type': 'text/x-gwt-rpc; charset=UTF-8',
        'X-GWT-Module-Base': 'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/',
        'X-GWT-Permutation': 'AEDAC353BD2C774915FB7B85744D4FFF',
        'Referer': f'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/?&feats={station_id}&type=S&name={station_name},%20{country}&title=WMO%20{att_value}%20for%20{station_name},%20{country}%20(WMO%20Station%20{station_id})&freq={frequency}&att={att_value}&years={year}&style=line&start={year}0101&end={year}1231',
        'Connection': 'keep-alive',
    }
    files_data = f'7|0|6|https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/|386CE0E7206C8C15FC85203A478BDF82|gov.usda.fas.ipad.cadre.gwt.client.CadreQuickGraphsService|getExtracts|java.lang.String/2004016611|type=S&freq=dekade&feats={station_id}&att={att_key}&start={year}0101&end={year}1231&years={year}&key={key}|1|2|3|4|1|5|6|'
    files_response = requests.post(
        'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/graphservice', verify=False,
        headers=files_headers, data=files_data, proxies=proxies)
    files = eval(files_response.text.split("//OK")[1])[1][0]
    print(files)

    value_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        'Content-Type': 'text/x-gwt-rpc; charset=UTF-8',
        'X-GWT-Module-Base': 'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/',
        'X-GWT-Permutation': 'AEDAC353BD2C774915FB7B85744D4FFF',
        'Referer': f'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/?&feats={station_id}&type=S&name={station_name},%20{country}&title=WMO%20{att_value}%20for%20Aihui,%20{country}%20(WMO%20Station%20{station_id})&freq={frequency}&att={att_value}&years={year}&style=line&start={year}0101&end={year}1231',
        'Connection': 'keep-alive',
    }

    value_data = f'7|0|6|https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/|386CE0E7206C8C15FC85203A478BDF82|gov.usda.fas.ipad.cadre.gwt.client.CadreQuickGraphsService|getGraphData|java.lang.String/2004016611|att={att_key}&files={files}|1|2|3|4|1|5|6|'
    value_response = requests.post(
        'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/graphservice', verify=False,
        headers=value_headers, data=value_data, proxies=proxies)
    try:
        value = eval(value_response.text.split("//OK")[1])[1][0]
        print(value)
        #     print(station_id,station_name)
        #         value = get_valuedata(station_id,station_name,country,year,att_key=att_key)
        value1 = value.split("|")[1]
        value2 = value1.split("=")[1].split(";")
        date_list = list()
        data_list = list()
        for v in value2:
            v_l = v.split(",")
            date = v_l[0]
            data = v_l[1]
            data_list.append(data)
            date_list.append(date)
            print(date, data)
        data_df = pd.DataFrame({'datetime': date_list, 'Precipitation(mm)': data_list})
        data_df['station_id'] = station_id
        print(f'****************爬取到的数据入库******************')
        data_df.to_sql(f'Precipitation{year}', engine, index=False, if_exists='append')
    except:
        error = f"stationid = {station_id},station_name = {station_name},返回值为 {value}"
        print(error)
        with open('./error.txt', 'a') as f:
            f.write(error)


def get_data_by_country(country,year):
    engine = create_engine('mysql+pymysql://root:123456@47.94.211.104:3306/wmo')
    sql = f"SELECT * FROM station_info_dis_final WHERE country = ' {country} '"
    df = pd.read_sql_query(sql, engine)
    print(df)
    df.apply(lambda row : get_valuedata(row['station_id'],row['city'].strip(),country,year),axis=1)



def command_generate():
    parser = argparse.ArgumentParser(description="根据国家来下载气象数据",
                                     epilog="python wmo.py -c CHINA")
    parser.add_argument("-c", "--country", help="国家英文名全大写")
    parser.add_argument("-y","--year",help="数据年限")
    args = parser.parse_args()
    country = args.country
    year = args.year
    get_data_by_country(country=country,year=year)


if  __name__ == "__main__":
    command_generate()