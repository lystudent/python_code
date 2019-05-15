# -*- coding: utf-8 -*-
# @Time    : 2019-04-23 12:00
# @Author  : liuyanming
# @Email   : 1420743191@qq.com
# @File    : ceshi_ip.py
# @Software: PyCharm

import requests
import ssl
from lxml import etree
ssl._create_default_https_context = ssl._create_unverified_context

#获取当前访问使用的IP地址网站
url="https://www.ipip.net/"

#设置代理，从西刺免费代理网站上找出一个可用的代理IP
proxies = {
    "http": "http://119.102.185.145:9999",
    "http": "http://119.102.184.200:9999",
    "http": "http://119.102.129.161:9999",
    "http": "http://119.102.130.193:9999",
    "http": "http://119.102.130.37:9999",
    "http": "http://110.52.235.145:9999",
    "http": "http://119.102.130.87:9999",
}#此处也可以通过列表形式，设置多个代理IP，后面通过random.choice()随机选取一个进行使用

#使用代理IP进行访问
res=requests.get(url,proxies=proxies,timeout=10)
status=res.status_code # 状态码
print(status)
# content=res.text
# html=etree.HTML(content)
# ip=html.xpath('//ul[@class="inner"]/li[1]/text()')[0]
# print("当前请求IP地址为："+ip)
