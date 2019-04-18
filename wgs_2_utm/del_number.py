# -*- coding: utf-8 -*-
# @Time    : 2019-04-16 15:57
# @Author  : liuyanming
# @Email   : 1420743191@qq.com
# @File    : del_number.py
# @Software: PyCharm


import pandas as pd
import argparse

def getsenven(bound):
    b = bound[1:-1].split(',')
    a = '('
    for i in range(4):
        key = b[i]
        #     print(key)
        if len(str(key.split('.')[0])) > 7:
            #             print (key)
            key = key[2:]
        #             print(key)
        a += f"{key},"
    a = a[:-1]
    a += ")"
    print(a)
    return a


def readcsv(inpath):
    """

    :param inpath:
    :return:
    """
    data = pd.read_csv(inpath)
    data['bounds'].apply(getsenven)
    out_path = inpath.replace(".csv","-new.csv")
    data.to_csv(out_path,index=False)

def command_generate():
    """
        wgs转utm
    """
    parser = argparse.ArgumentParser(description="祛除多余数字",
                                     epilog="python del_number.py -i soybean.csv")
    parser.add_argument("-i", "--input", help="csv路径")
    args = parser.parse_args()
    in_path = args.input
    readcsv(in_path)


if __name__ == "__main__":
    command_generate()
