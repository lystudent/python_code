# -*- coding: utf-8 -*-
# @Time    : 2019-04-04 21:46
# @Author  : liuyanming
# @Email   : 1420743191@qq.com
# @File    : pachong.py
# @Software: PyCharm


import requests
from bs4 import BeautifulSoup


def demo():

    cookie = '_ga=GA1.2.1172425064.1551165821; _gid=GA1.2.929553204.1554389339'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36',
        'Connection': 'keep-alive',
        'accept': '*/*',
        'Cookie': cookie}
    url = 'https://gis.pecad.fas.usda.gov/WmoStationExplorer/wmostationexplorer_v20170101/wmoservice'
    res = requests.get(url, headers=headers).text
    # soup = BeautifulSoup(res, 'html.parser')
    # print(soup)


if __name__ == '__main__':
    demo()
