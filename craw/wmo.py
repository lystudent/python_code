# -*- coding: utf-8 -*-
# @Time    : 2019-04-18 17:37
# @Author  : liuyanming
# @Email   : 1420743191@qq.com
# @File    : wmo.py
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
from concurrent.futures import ProcessPoolExecutor
import argparse

engine = create_engine('mysql+pymysql://root:123456@47.94.211.104:3306/wmo')


def station_id_to_table(like_id: int) -> None:
    """
     根据likeID获取站点信息，包括站点id，站点名称，所属国家
    :param like_id:
    :return:
    """
    pattern = re.compile(r'\n.*\n', re.UNICODE)
    curl_cmd = f"curl 'https://gis.pecad.fas.usda.gov/WmoStationExplorer/wmostationexplorer_v20170101/wmoservice' -H 'Cookie: _ga=GA1.2.1172425064.1551165821; _gid=GA1.2.265851413.1554707421' -H 'Origin: https://gis.pecad.fas.usda.gov' -H 'Accept-Encoding: gzip, deflate, br' -H 'Accept-Language: en,zh-CN;q=0.9,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36' -H 'Content-Type: text/x-gwt-rpc; charset=UTF-8' -H 'Accept: */*' -H 'X-GWT-Module-Base: https://gis.pecad.fas.usda.gov/WmoStationExplorer/wmostationexplorer_v20170101/' -H 'X-GWT-Permutation: 72DC3FDA117244501BBF8948D77BA3C6' -H 'Referer: https://gis.pecad.fas.usda.gov/WmoStationExplorer/' -H 'Connection: keep-alive' --data-binary '7|0|6|https://gis.pecad.fas.usda.gov/WmoStationExplorer/wmostationexplorer_v20170101/|EB1B4D68CD7779EEB3DC46E108C6DBC9|gov.usda.fas.ipad.wse.gwt.client.WmoStationExplorerService|searchStations|java.lang.String/2004016611|1/query?where=station_id+like+%27%25{like_id}%25%27&outFields=station_na%2Ccountry%2Cstation_id&returnGeometry=false&orderByFields=station_na&f=pjson|1|2|3|4|1|5|6|' --compressed"
    station_info = subprocess.getoutput(curl_cmd)
    station_info_key = station_info.split("//OK")[1]
    station_info_key = re.sub(pattern, "", station_info_key, count=0)
    station_info_key_list = eval(station_info_key)
    station_need = station_info_key_list[-3][2:]
    ak_list = list()
    bk_list = list()
    ck_list = list()
    for keys in station_info_key_list[-3][2:]:
        city = keys.split(',')[0]
        delcity = keys.split(',')[1]
        station_id = delcity[delcity.rfind('(') + 1:-1]
        country = delcity[:delcity.rfind('(')]
        #         print(city,country,station_id)
        ak_list.append(city)
        bk_list.append(country)
        ck_list.append(station_id)
        data1 = pd.DataFrame({'station_id': ck_list, 'country': bk_list, 'city': ak_list})
        data1['like_id'] = like_id
        data1.to_sql('station_info1', engine, index=False, if_exists='append')


# for i in range(99):
#     """
#     根据likeID获取站点信息，包括站点id，站点名称，所属国家
#     """
#     print(f"第{i}个数据开始爬取")
#     # 循环10-98的id，匹配获得站点数据
#     station_id_to_table(i)

#
# def getdata(station_id,station_name):
#     """
#     curl版本的数据获取
#     :param station_id:
#     :param station_name:
#     :return:
#     """
#     cmd = "curl 'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/graphservice' -H 'Cookie: _ga=GA1.2.1172425064.1551165821; _gid=GA1.2.1959471478.1555316255' -H 'Origin: https://gis.pecad.fas.usda.gov' -H 'Accept-Encoding: gzip, deflate, br' -H 'Accept-Language: en,zh-CN;q=0.9,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36' -H 'Content-Type: text/x-gwt-rpc; charset=UTF-8' -H 'Accept: */*' -H 'X-GWT-Module-Base: https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/' -H 'X-GWT-Permutation: AEDAC353BD2C774915FB7B85744D4FFF' -H 'Referer: https://gis.pecad.fas.usda.gov/CadreQuickGraphs/?&feats={station_id}&type=S&name={station_name},%20CHINA&title=WMO%20Cumulative%20Precipitation%20for%20{station_name},%20CHINA%20(WMO%20Station%20{station_id})&freq=Daily&att=Cumulative%20Precipitation&years=2019&style=line&start=20190309&end=20190407' -H 'Connection: keep-alive' --data-binary '7|0|5|https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/|386CE0E7206C8C15FC85203A478BDF82|gov.usda.fas.ipad.cadre.gwt.client.CadreQuickGraphsService|getKey|java.lang.String/2004016611|1|2|3|4|1|5|0|' --compressed"
#     b_res = eval(subprocess.getoutput(cmd).split("//OK")[1])[1]
#     dat_cmd = f"curl 'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/graphservice' -H 'Cookie: _ga=GA1.2.1172425064.1551165821; _gid=GA1.2.1959471478.1555316255' -H 'Origin: https://gis.pecad.fas.usda.gov' -H 'Accept-Encoding: gzip, deflate, br' -H 'Accept-Language: en,zh-CN;q=0.9,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36' -H 'Content-Type: text/x-gwt-rpc; charset=UTF-8' -H 'Accept: */*' -H 'X-GWT-Module-Base: https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/' -H 'X-GWT-Permutation: AEDAC353BD2C774915FB7B85744D4FFF' -H 'Referer: https://gis.pecad.fas.usda.gov/CadreQuickGraphs/?&feats={station_id}&type=S&name={station_name},%20CHINA&title=WMO%20Cumulative%20Precipitation%20for%20{station_name},%20CHINA%20(WMO%20Station%20{station_id})&freq=Daily&att=Cumulative%20Precipitation&years=2019&style=line&start=20190309&end=20190407' -H 'Connection: keep-alive' --data-binary '7|0|6|https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/|386CE0E7206C8C15FC85203A478BDF82|gov.usda.fas.ipad.cadre.gwt.client.CadreQuickGraphsService|getExtracts|java.lang.String/2004016611|type=S&freq=day&feats={station_id}&att=Cumul%20precip&start=20190309&end=20190407&years=2019&key={b_res[0]}|1|2|3|4|1|5|6|' --compressed"
#     dat_res = eval(subprocess.getoutput(dat_cmd).split('//OK')[1])[1]
#     data_cmd = f"curl 'https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/graphservice' -H 'Cookie: _ga=GA1.2.1172425064.1551165821; _gid=GA1.2.1959471478.1555316255' -H 'Origin: https://gis.pecad.fas.usda.gov' -H 'Accept-Encoding: gzip, deflate, br' -H 'Accept-Language: en,zh-CN;q=0.9,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36' -H 'Content-Type: text/x-gwt-rpc; charset=UTF-8' -H 'Accept: */*' -H 'X-GWT-Module-Base: https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/' -H 'X-GWT-Permutation: AEDAC353BD2C774915FB7B85744D4FFF' -H 'Referer: https://gis.pecad.fas.usda.gov/CadreQuickGraphs/?&feats={station_id}&type=S&name={station_name},%20CHINA&title=WMO%20Cumulative%20Precipitation%20for%20{station_name},%20CHINA%20(WMO%20Station%20{station_id})&freq=Daily&att=Cumulative%20Precipitation&years=2019&style=line&start=20190309&end=20190407' -H 'Connection: keep-alive' --data-binary '7|0|6|https://gis.pecad.fas.usda.gov/CadreQuickGraphs/cadrequickgraphs_v20170101/|386CE0E7206C8C15FC85203A478BDF82|gov.usda.fas.ipad.cadre.gwt.client.CadreQuickGraphsService|getGraphData|java.lang.String/2004016611|att=Cumul%20precip&files={dat_res[0]}|1|2|3|4|1|5|6|' --compressed"
#     data_res = eval(subprocess.getoutput(data_cmd).split('//OK')[1])
#     return data_res[1]

def get_valuedata(station_id, station_name, country, year, att_key="Cumul%20precip"):
    """
    根据气象站点id，站点名称，国家，时间，类别，获取数据
    :param station_id:
    :param station_name:
    :param country:
    :param year:
    :param att_key:
    :return:
    """
    proxies ={
        "http": "http://119.102.25.252:9999",
        "http" : "http://116.208.54.10:9999"
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
    value = eval(value_response.text.split("//OK")[1])[1][0]
    # print(value)
    return value


def get_data_by_station(station_id,station_name,country,year,att_key="Average%20temp"):
    try:
        print(station_id,station_name)
        value_new = get_valuedata(station_id,station_name,country,year,att_key=att_key)
        value1 = value_new.split("|")[1]
        value2 = value1.split("=")[1].split(";")
        date_list=list()
        data_list=list()
        for v in value2:
            v_l = v.split(",")
            date = v_l[0]
            data = v_l[1]
            data_list.append(data)
            date_list.append(date)
            print(date,data)
        data_df = pd.DataFrame({'datetime':date_list,'AverageTemp(℃)':data_list})
        data_df['station_id']=station_id
        data_df.to_sql(f'AverageTemp{year}',engine,index=False,if_exists='append')
    except Exception as e:
        error = f"stationid = {station_id},station_name = {station_name},错误信息为 {e}"
        print(error)
        with open('./error.txt', 'a') as f:
            f.write(error)


def get_data_by_country(country,year):
    engine = create_engine('mysql+pymysql://root:123456@47.94.211.104:3306/wmo')
    sql = f"SELECT * FROM station_info_dis_final WHERE country = ' {country} '"
    sql = "SELECT * FROM station_info_dis_final WHERE country in (' BRAZIL ',' ARGENTINA ',' CHINA ',' CANADA ',' UKRAINE ',' UNITED STATES ',' RUSSIA ')"
    df = pd.read_sql_query(sql, engine)
    print(df)
    df.apply(lambda row : get_data_by_station(row['station_id'],row['city'].strip(),country,year),axis=1)



def get_data_by_year(year):
    engine = create_engine('mysql+pymysql://root:123456@47.94.211.104:3306/wmo')
    sql = "SELECT * FROM station_info_dis_final_1 WHERE country in ('BRAZIL','ARGENTINA','CHINA','CANADA','UKRAINE','UNITED STATES','RUSSIA')"
    df = pd.read_sql_query(sql, engine)
    df = df[912:]
    print(df)
    df.apply(lambda row : get_data_by_station(row['station_id'],row['city'].strip(),row['country'],year),axis=1)
# def command_generate():
#     parser = argparse.ArgumentParser(description="根据国家来下载气象数据",
#                                      epilog="python wmo.py -c CHINA")
#     parser.add_argument("-c", "--country", help="国家英文名全大写")
#     parser.add_argument("-y","--year",help="数据年限")
#     args = parser.parse_args()
#     country = args.country
#     year = args.year
#     if str(country).__contains__(','):
#         countrys = country.split(',')
#         for c in countrys:
#             get_data_by_country(country=c, year=year)
#     else:
#         get_data_by_country(country=country, year=year)


def command_generate():
    parser = argparse.ArgumentParser(description="根据国家来下载气象数据",
                                     epilog="python wmo.py -y 2017")
    # parser.add_argument("-c", "--country", help="国家英文名全大写")
    parser.add_argument("-y","--year",help="数据年限")
    args = parser.parse_args()
    year = args.year
    get_data_by_year(year)

# sql = f"SELECT * FROM station_info_dis_final WHERE country = ' ARGENTINA '"
# df = pd.read_sql_query(sql, engine)
#
#
# get_data_by_country("ARGENTINA")
# with ProcessPoolExecutor(max_workers=8) as executor:
#     executor.map(get_data_by_station, df.station_id,df.city,df.country)


if __name__ == "__main__":
    command_generate()
    # year = '2015'
    # country = "UNITED STATES"
    # sql = f"SELECT * FROM station_info_dis_final WHERE country = ' {country} '"
    # df = pd.read_sql_query(sql, engine)
    # df['year'] = '2015'
    # new_df = df[124:]
    # new_df.apply(lambda row : get_data_by_station(row['station_id'],row['city'].strip(),country,year),axis=1)

    # with ProcessPoolExecutor(max_workers=40) as executor:
    #     executor.map(get_data_by_station, df.station_id, df.city, df.country,df.year)