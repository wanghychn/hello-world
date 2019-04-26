# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 14:16:09 2019

@author: wanghy
"""
"""
当前还存在的问题
1、只根据标点符合、和、与拆分，并没有尽可能去除多余信息，因为不确定这些信息是否有用
2、表中存在邮编信息，格式稍显混乱，邮编前还存在地区或其他无用信息
3、可能存在一些没有考虑周全的情况，导致机构拆分存在误差
"""

import db_tool
from collections import OrderedDict
import re
import time

t1 = time.time()

#数据准备工作，从数据库中取出数据
db = db_tool.dbtool("temp","root","Root123456.","39.98.161.93",3307)
db.start_conn()
wb = db.search("SELECT * FROM people_china_trance_pi")

institutional_identification = ["实验室","医院","中心","学院","研究所","科研所","大学"]
symbol_identification = [",","，","/","与","和",";","；","\\","、"]

def remove_zip_code(string):
    '''
    去除邮编信息
    '''
    record = re.compile("(.*[心所院室系科校部学]+).{0,3}[0-9]{6}$").findall(string)
    if record == [] or record == [""]:
        record = string
    else:
        if len(record)!=1:
            print("complet error")
        record = record[0]
    if len(record)<=10:
        test = re.compile("\d{6}").findall(record[-6:])
        if test != []:
            record = ""
    return record

def data_cleaning(string):
    '''
    数据清洗
    '''
    special_node = "(){}[]【】（）“”‘’\'\'\"\""#特殊标点
    special_tag = "1234567890ABCDabcd"
    string = string.strip()#去除两侧空字符
    if string[0] in special_tag and string[1] not in special_tag:
        string = string[1:]
    string = string.strip()#去除两侧空字符
    string = re.sub("^[\(（]?[\d{1}a-zA-Z一二三四五六七八九]?[\)）]","",string)#去除开头处没有的括号标识
    string = string.strip()
    string = re.sub("^[-.*]","",string)#去除特殊情况
    string = string.strip()
    string = re.sub("【更正】","",string)
    string = string.strip()#再次去除两侧的空字符
    #检测字符串两边是否存在么用的特殊符号
    while string[0] in special_node and string[-1:] in special_node and string[-1:]==special_node[special_node.index(string[0])+1]:
        string = string[1:len(string)-1]
        string.strip()
    return string

def get_connect_tag(string,start,stop):
    '''
    获取两个字段之间的连接符
    '''
    pat=start+"(.?)"+stop
    tag=re.compile(pat).findall(string)[0]
    return tag

def handle_hospital_without_keshi(string):
    '''
    去掉医院后面的科室信息
    '''
    temp_list = re.compile(".*?医院[\)）】\]]?").findall(string)
    result = ""
    for i in temp_list:
        result+=i
    return result

def handle_str_by_spenode(string):
    '''
    处理特殊标点符号连接的字符串
    '''
    split_list = re.split("[,/;，。；、]",string)#这里不能根据”.“来分割，许多英文名里面带.但是是一个词
    result = ""
    for item in split_list:
        item = item.strip()
        item = remove_zip_code(item)#尝试在根据标点符号分割的时候去除邮编信息
        if item != "":
            #处理医院的特殊情况，精确到医院去掉科室
            if "医院" in item:
                item = handle_hospital_without_keshi(item)
            result = result+",,,"+item
    return result[3:]

def handle_str_by_and(string):
    '''
    处理了和、与连接的字符串
    '''
    ori_string = string
    temp_list = re.compile("(.*?[心所院室系科校部][\)）】\]]?)和").findall(string)#这个地方不能根据”学“划分，某学和某学研究所
    if temp_list != []:
        string = ""
        for i in temp_list:
            i = remove_zip_code(i)
            string = string+",,,"+i
        if len(string)-len(temp_list)*2<len(ori_string):
            string = string[3:]+",,,"+ori_string[len(string)-len(temp_list)*2:]
    temp_list = re.compile("(.*?[心所院室系科校部][\)）】\]]?)与").findall(string)
    if temp_list != []:
        string = ""
        for i in temp_list:
            i = remove_zip_code(i)
            string = string+",,,"+i
        string = string[3:]+",,,"+ori_string[len(string)-len(temp_list)*2:]
    return string

if __name__ == "__main__":
    for item in wb:
        #数据清洗
        if item["pi_cn"] != None:
            item["pi_cn"] = data_cleaning(item["pi_cn"])
            #item["pi_cn_2"] = str((re.compile("(.{3})和").findall(item["pi_cn"]))).strip("\[\]")#用来统计和、与前面的词是什么
            item["pi_cn"] = handle_str_by_spenode(item["pi_cn"])
            item["pi_cn"] = handle_str_by_and(item["pi_cn"])

    newwb = []
    for i in wb:
        newwb.append(list(i.values()))
    #插入数据库
    table = OrderedDict()
    table["id"] = "varchar(500)"
    table["pi_en"] = "varchar(500)"
    table["pi_cn"] = "varchar(500)"
    table["pi_cn_1"] = "varchar(500)"
    table["pi_cn_2"] = "varchar(500)"
    db.data_import(newwb,"people_china_trance_pi_result",table)
t2 = time.time()
print("时间为："+str(t2-t1))