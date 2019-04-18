# -*- coding: utf-8 -*-
# @Time    : 2019-04-02 17:40
# @Author  : liuyanming
# @Email   : 1420743191@qq.com
# @File    : shp_wgs_2_utm.py
# @Software: PyCharm

import os
import geopandas as g_pd
import subprocess as cmd
import concurrent.futures as con
import argparse


def wgs_to_UTM_shp(inpath, outpath=None):
    """
    对shape文件进行坐标转换，wgs转utm
    :param inpath: 输入shape文件路径
    :param outpath: 写出shape文件路径
    :return:
    """
    if outpath == None:
        outpath = inpath.replace(".shp", "_UTM.shp")
    outpath_root = os.path.split(outpath)[0]
    if not os.path.exists(outpath_root):
        os.makedirs(outpath_root)
    tmp = g_pd.read_file(inpath)
    coords_list = list(tmp.geometry[0].centroid.coords)
    x, y = coords_list[0]
    x = int(x)
    if x < 0:   # acquire UTM zone
        utm = int(x/6) + 30
    else:
        utm = int(x/6) + 31
    if y > 0:
        EPSG_num = "326" + str(utm) # acquire EPSG
    else:
        EPSG_num = "327" + str(utm)
    cmd.check_output("ogr2ogr -t_srs EPSG:{} {} {}".format(EPSG_num, outpath, inpath), shell=True)
    return outpath


def multi_wgs_to_UTM_shp(in_path, out_path=None):
    """
    对给定目录下所有的shape文件进行坐标转换，wgs转shp
    :param in_path:
    :param out_path:
    :return:
    """
    out_path = out_path if out_path else in_path
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    os.chdir(in_path)
    files = cmd.check_output("find . -name \*.shp", shell=True).decode().splitlines()
    files_in = []
    files_out = []
    for shpfile in files:
        file_path, file_name = os.path.split(shpfile)
        if "_UTM.shp" in file_name:
            continue
        files_in.append(shpfile)
        files_out.append(os.path.join(file_path.replace('./',out_path+'/'), file_name.replace(".shp", "_UTM.shp")))
    with con.ProcessPoolExecutor() as executor:
        executor.map(wgs_to_UTM_shp, files_in, files_out)
    return out_path

def command_generate():
    """
        wgs转utm
    """
    parser = argparse.ArgumentParser(description="创建工作区，发布tif影像，发布shp文件，修改图层样式，发布图层组",
                                     epilog="python wgs_to_UTM_shp.py -i soybean.shp -o soybean_UTM.shp")
    parser.add_argument("-i", "--input", help="geoserve服务r的ip和port")
    args = parser.parse_args()
    in_path = args.input
    wgs_to_UTM_shp(in_path)


if __name__ == "__main__":
    command_generate()

