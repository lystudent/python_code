# -*- coding: utf-8 -*-
# @Time    : 2019-04-05 00:21
# @Author  : liuyanming
# @Email   : 1420743191@qq.com
# @File    : usgs_crwa.py
# @Software: PyCharm

# -*- coding: UTF-8 -*-

#   导入http请求包 -->用于http请求
import requests
#   导入类选择器解析包 -->用于请求后结果解析
from bs4 import BeautifulSoup
#   导入json包 -->用于json格式操作
import json
#   正则包
import re
#   时间函数-->休眠
import time
#   闭包
from contextlib import closing
#   引入系统包-->为java提供函数调用
import sys
from typing import Dict, Tuple, List, Optional

'''
    @author chenbaining
    Date: 2019/3/16 上午9:17
    @since Python 3.6
    背景：从ers.cr.usgs.gov网站下载哨兵遥感影像数据
    实现：1.网站只允许登陆后选择影像下载，所以需要先进行登陆
         1.1 登陆需要302跳转,禁用302自动跳转
         2.选择影像区域和影像数据的时间，进行提交到下一步
         3.选择影像类型进行下一步
         4.选择云量进行下一步 
         5.提交数据，选择影像数据格式进行下载 [下载时间过长，需要有提示]
         6.下载 [需要循环下载所有影像数据]
         以上所有步骤都为单独的def，最后提供一个统一出口
    启动：文件内，请直接点击"__main__"运行即可
         java调用，请运行"img_main"方法，去掉sys.argv[1]等众多参数注释
'''

#   全局request 自动维护cookie
conn: requests.Session = requests.session()


#   登陆模块-->当前项目登陆分为三块
#   1.先跳转到登陆页拿token
#   2.后将拿到的token和账号密码进行post提交
#   3.成功后模拟302跳转，进行页面请求，判断页面返回值
def login(request_username: str, request_password: str) -> Optional[str]:
    assert isinstance(request_username, str) and isinstance(request_password, str)
    result_str = None
    #   跳转到登陆页
    res = conn.get('https://ers.cr.usgs.gov/login')
    #   获取请求结果，以html形式解析
    soup = BeautifulSoup(res.text, 'html.parser')
    # 获取页面中token值
    token = soup.select('#csrf_token')[0]['value']
    # print(token)

    headers = {
        "Host": "ers.cr.usgs.gov",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "https://ers.cr.usgs.gov/login",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36"
    }
    #   登陆所需要的请求参数
    d = {'username': request_username, 'password': request_password, 'csrf_token': token}
    #   进行参数提交 禁止302跳转，手动启动跳转
    res = conn.post('https://ers.cr.usgs.gov/login/', headers=headers, data=d, allow_redirects=False)
    #  如果状态为302，正确
    if res.status_code == 302:
        res = conn.get('https://ers.cr.usgs.gov/')
        soup = BeautifulSoup(res.text, 'html.parser')
        result_str = soup.select('div#credentialContainer > span')[0].text.strip()
    # 返回值
    return result_str


#   阶段1-->上传shpfile、转换坐标点、设置时间
def stage_1(request_file_path: str, request_data_start: str, request_data_end: str) -> None:
    assert isinstance(request_file_path, str) and isinstance(request_data_start, str)
    assert isinstance(request_data_end, str)
    #   首先需要优先访问上传页面获取token
    res = conn.get('https://earthexplorer.usgs.gov/upload/shapefile')
    #   正则匹配到token
    pattern = re.compile(r'X-Csrf-Token(.*)\'')
    re_result = pattern.search(res.text).group()
    token = re_result.replace('X-Csrf-Token', '').replace('\'', '').replace(',', '').strip()
    print('token:', token)

    # 因为是js异步加载，必须加上X-Requested-With 变成XHR请求。X-Csrf-Token是token，页面中带的
    headers = {
        'X-Requested-With': 'XMLHttpRequest',
        'X-Csrf-Token': token,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
    }
    files = {'files': open(request_file_path, 'rb')}  # 文件
    res = conn.post('https://earthexplorer.usgs.gov/upload/shapefile', headers=headers, files=files)

    print('打印', res, res.text)

    json_data = json.loads(res.text)
    #   读取其中的坐标点
    dis = [(item.get('lat', 'NA'), item.get('lng', 'NA')) for item in json_data['coordinates']]
    #   循环输出坐标点并四舍五入保留小数点后四位
    test_dict = {"tab": 1, "destination": 2, "coordinates": None, "format": "dms", "dStart": request_data_start,
                 "dEnd": request_data_end, "searchType": "Std", "num": "100", "includeUnknownCC": "1", "maxCC": 100,
                 "minCC": 0, "months": ["", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"],
                 "pType": "polygon"}

    point_list: List = [
        {'c': str(index),
         'a': f"{float(elem[0]):.4f}",
         'o': f"{float(elem[1]):.4f}"
         }
        for index, elem in enumerate(dis)
    ]
    test_dict['coordinates'] = point_list
    request_json = json.dumps(test_dict)

    print(request_json)

    #   coordinates坐标点需要动态填写，剩下为固定格式
    d = {
        'data': request_json
    }
    res = conn.post('https://earthexplorer.usgs.gov/tabs/save', data=d)
    status = res.text
    if int(status) == 1:
        print(f'stage_1 状态为{"Success"}')
    else:
        print(f'stage_1 状态为{"Failure"}')


#   阶段2-->选择数据集 哨兵影像
def stage_2() -> None:
    # res = conn.post('https://earthexplorer.usgs.gov/ajax/datasetnotification',
    #                 data={'collection_id': '10880'})
    # print('stage_2', res.text)
    #   哨兵影像，固定写法，如果需要其他影像数据，请从get_img_list方法中获取
    request_data = '{"tab":2,"destination":3,"cList":["10880"],"selected":10880}'
    d = {
        'data': request_data
    }
    res = conn.post('https://earthexplorer.usgs.gov/tabs/save', data=d)
    status = res.text
    if int(status) == 1:
        print(f'stage_2 状态为{"Success"}')
    else:
        print(f'stage_2 状态为{"Failure"}')


#   阶段3-->选择云量
def stage_3(request_cloud_amount: str) -> None:
    #   云量选择 select_18696_5=""为 ALL、select_18696_5="0"-->10% 、select_18696_5="1"-->20% 、select_18696_5="9"-->100%
    request_data = '{"tab":3,"destination":4,"criteria":{"10880":{"select_18696_5":["' + request_cloud_amount + '"],"select_18697_3":[""],"select_22067_3":[""]}},"selected":"10880"}'
    d = {
        'data': request_data
    }
    res = conn.post('https://earthexplorer.usgs.gov/tabs/save', data=d)
    status = res.text
    if int(status) == 1:
        print(f'stage_3 状态为{"Success"}')
    else:
        print(f'stage_3 状态为{"Failure"}')


#   影像列表寻找影像下载，循环获取到影像
def stage_4_img_list() -> List:
    result_img_id_list = []
    current_page_num = 1
    total_page_num = 1
    while current_page_num <= total_page_num:
        d = {
            'collectionId': '10880',
            'pageNum': current_page_num
        }
        res = conn.post('https://earthexplorer.usgs.gov/result/index', data=d)
        soup = BeautifulSoup(res.text, 'html.parser')
        #  获取到的id放入集合中
        for img_id in soup.select('tbody > tr'):
            result_img_id_list.append(img_id['data-entityid'])

        # 获取总页数
        for request_total_page in soup.select('select#pageSelector_10880 >option'):
            total_page_num = int(request_total_page.text.strip())

        # 当前页需要加1
        current_page_num += 1
    return result_img_id_list


#   下载影像
def download_img(request_img_id_list: List) -> None:
    assert isinstance(request_img_id_list, List)
    for img_id in request_img_id_list:
        url = 'https://earthexplorer.usgs.gov/download/options/10880/' + img_id
        print('\r当前影像链接[%s]' % url)
        res = conn.post(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        # soup = BeautifulSoup(open('download_img.html').read(), 'html.parser')
        #   需要解析数据时，先下载到本地，本地解析通过后，再注释掉当前代码
        # fw = open("download_img.html", 'w')
        # fw.write(res.text)

        n = 0
        for img_url in soup.select('div > input'):
            onclick_url = img_url['onclick']
            # 根据数据分析组要求获取STANDARD数据
            if onclick_url.find('STANDARD') > 0:
                result_url = onclick_url.replace('window.location=', '').replace('\'', '')
                #    请求url 禁止跳转
                res = conn.post(result_url, allow_redirects=False)
                if res.status_code == 302:
                    # 获取重定向的地址
                    location = res.headers['Location']
                    print('当前影像跳转后的地址[%s]' % location)
                    # 从地址中，截取文件的名称，作为下载后文件的名称
                    file_name = location[0:location.find('?')].split('/')[-1]
                    #   执行重定向后的地址，请求数据
                    download_redirect(file_name, location, n + 1)
                    break
        #   简单反防爬
        time.sleep(6)
    print("任务执行完毕")


#   🌟获取所有类型影像列表[暂时没有用][有空腾出时间处理此列表可实现下载其他类型数据]
#   如果需要进行影像解析，参考sampleList.txt文件，或者请求https://earthexplorer.usgs.gov/ajax/getdatasets地址
def get_img_list() -> None:
    # res = conn.get('https://earthexplorer.usgs.gov/ajax/getdatasets')
    # soup = BeautifulSoup(res.text, 'html.parser')

    soup = BeautifulSoup(open('sampleList.txt').read())
    # for root in soup.select('ul#dataset-menu'):
    #     print(root)
    #     for img_list in root.select('li'):
    #         try:
    #             print(img_list.select('span[class="folder"] > div[class="categoryHitArea'))
    #         except:
    #             pass
    result = soup.find_all('script')[0].text.strip()
    # 匹配开始时的索引，获取字符串长度，自动去掉EE.dataset.datasets=
    star_index = len('EE.dataset.datasets=')
    # 匹配结束的索引（长度）
    end_index = result.find('EE.dataset.customized')
    #   将结果去掉所有；号，还原为json型字符串
    json_data = result[star_index:end_index].replace(';', '').replace('\n', '')
    print(json_data)
    #   变成json对象
    json_obj = json.dumps(json_data)
    #   接下来解析json拿到影像列表数据


#   减少人工出错，模仿java switch case语句实现转换
def cloud_amount(request_var: str) -> str:
    assert isinstance(request_var, str)
    return {
        'all': '',
        '10%': '0',
        '20%': '1',
        '30%': '2',
        '40%': '3',
        '50%': '4',
        '60%': '5',
        '70%': '6',
        '80%': '7',
        '90%': '8',
        '100%': '9',
    }.get(request_var, 'error')


#   由于下载时间较长，所以选择分段下载并提供提示
def download_redirect(file_name: str, request_url: str, req_number: int) -> None:
    assert isinstance(file_name, str) and isinstance(request_url, str)
    assert isinstance(req_number, int)
    with closing(conn.get(request_url, stream=True)) as response:
        # 单次请求最大值
        chunk_size = 1024
        # 内容体总大小
        content_size = int(response.headers['content-length'])
        data_count = 0
        with open(file_name, "wb") as code:
            for data in response.iter_content(chunk_size=chunk_size):
                code.write(data)
                data_count = data_count + len(data)
                now_jd = (data_count / content_size) * 100
                print("\r正在下载第[%d]个文件，文件下载进度：%d%%(%d/%d) - %s" % (
                    req_number, now_jd, data_count, content_size, request_url), end=" ")


def img_main():
    # #   登陆的账号
    # username = sys.argv[1]
    # #   登陆的密码
    # password = sys.argv[2]
    # #   shp文件路径
    # file_path = sys.argv[3]
    # #   影像开始时间
    # img_data_start = sys.argv[4]
    # #   影像结束时间
    # img_data_end = sys.argv[5]
    # #   选择云量
    # img_cloud_amount = sys.argv[6]

    # 登陆的账号
    username = 'xinghengkeji'
    # 登陆的密码
    password = 'qwe123456'
    # 需要下载的影像的shp文件
    file_path = './0404_02.zip'
    #   影像开始时间
    img_data_start = '11/15/2017'
    #   影像结束时间
    img_data_end = '01/10/2018'
    #   云量选择
    img_cloud_amount = '30%'
    # 登陆模块
    result_str = login(username, password)
    print(f'\r欢迎{result_str}登录')
    # 框选地块和选择日期
    stage_1(file_path, img_data_start, img_data_end)
    # # 选择数据类型
    stage_2()
    # # 选择云量
    stage_3(cloud_amount(img_cloud_amount))
    # 选择要下载的影像
    img_id_list = stage_4_img_list()
    print(f'需要下载的影像 共计[{len(img_id_list)}] - {img_id_list}')
    # 下载影像
    download_img(img_id_list)


# 启动方法
if __name__ == '__main__':
    img_main()
