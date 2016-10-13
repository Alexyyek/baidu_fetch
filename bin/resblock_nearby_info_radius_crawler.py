#coding: utf8

import os
import shutil
import sys
sys.path.append("../conf")
import urllib2
import json
import time
import logging
import thread
import pdb
import conf
import copy
from threading import Thread

class HouseNearbyInfoCreator:

    def __init__(self, app_key, retry_limit, query_lst, log_dir):
        
        self.app_key = app_key
        self.retry_limit = retry_limit
        self.query_lst = query_lst
        date_str = time.strftime('%Y%m%d',time.localtime(time.time()))
        logging.basicConfig(level=logging.INFO,\
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',\
                            datefmt='%a, %d %b %Y %H:%M:%S',\
                            filename= log_dir + "/%s.run_api.log" % date_str, \
                            filemode='w')

    def fetch_house_coordinate(self, addr):
        """
        通过百度地图API获取地址的坐标, 如成功返回经纬度，否则返回(-1,-1)
        """
        key = self.app_key
        key_index = 0
        content = ""
        api_url = "http://api.map.baidu.com/geocoder/v2/?address=%s&output=json&ak=%s" % (addr, key[key_index])

        while(key_index < len(key)):
            retry_cnt = 0
            while(retry_cnt <= self.retry_limit):
                try:
                    content = urllib2.urlopen(api_url, timeout=3).read()
                    isValid = eval(content)
                    if isValid['status'] != 0:
                        retry_cnt += 1
                        continue
                    rlt_dic = json.loads(content)
                    lng = round(rlt_dic["result"]["location"]["lng"], 3)
                    lat = round(rlt_dic["result"]["location"]["lat"], 3)
                except Exception,e:
                    print api_url, content, e
                    logging.error("request: %s, response: %s, err: %s" % (api_url, content, e))
                    retry_cnt += 1
                    continue
                return (lng, lat)
            key_index += 1
            logging.info("inValid key : %s, retry_time : %s, Changing key : %s" % (key[key_index -1], retry_cnt, key[key_index]))
            api_url = "http://api.map.baidu.com/geocoder/v2/?address=%s&output=json&ak=%s" % (addr, key[key_index])
        return (-1, -1)

    def fetch_nearby_detail_info(self, lng_lat_tpl, query, radius):
        """
        通过百度地图API，根据输入的地址和query(地铁站，公交站，医院等关键字)查询附近的配套设施及距离
        """
        key = self.app_key
        lng = lng_lat_tpl[0]
        lat = lng_lat_tpl[1]

        content = ""
        page_num = 0
        key_index = 0
        rlt_detail = []

        while(key_index < len(key)):
            retry_cnt = 0
            while 1:
                while (retry_cnt <= self.retry_limit):
                    '''
                    默认page_size=10，最大可修改为20，遍历page_num至返回为空循环结束
                    '''
                    api_url = "http://api.map.baidu.com/place/v2/search?query=%s&page_size=20&page_num=%s&scope=2&location=%s,%s&radius=%s&output=json&ak=%s" % \
                             (query, page_num, lat, lng, radius, key[key_index])
                    try:
                        content = urllib2.urlopen(api_url, timeout=3).read()
                        isValid = eval(content)
                        if isValid['status'] != 0:
                            retry_cnt += 1
                            continue
                        rlt_dic = json.loads(content)
                        rlt_res = rlt_dic["results"]
                        if rlt_res:
                            rlt_detail.append(rlt_res)
                            page_num += 1
                            retry_cnt = 0
                        else:
                            return rlt_detail
                    except Exception,e:
                        print api_url, content, e
                        logging.error("request: %s, response: %s, err: %s" % (api_url, content, e))
                        retry_cnt += 1
                        continue
                break
            key_index += 1
            if key_index < len(key):
                logging.info("inValid key : %s, retry_time : %s, Changing key : %s" % (key[key_index -1], retry_cnt, key[key_index]))
        return -1

    def fetch_house_nearby_info_mul(self, input_buffer, output_fname):
        """
        该方法适用于多线程方式的任务
        """
        output_file = open(output_fname, "w")
        processed_cnt = 0
        for line in input_buffer:
            if processed_cnt % 100 == 0:
                print "%s,processed: %s, time: %s" % (output_fname, processed_cnt, time.time())
                logging.info("%s, processed: %s, time: %s" % (output_fname, processed_cnt, time.time()))
            row = line.strip().split("\t")
            house_id = row[6]
            house_name = row[1] + row[3] + row[5].strip() + row[7].replace(' ','')
            if "测试" in house_name : continue
            house_name.replace(' ','')
            lng_lat_tpl = self.fetch_house_coordinate(house_name)
            if lng_lat_tpl == (-1, -1): continue
            output_lst = [house_id, house_name, str(lng_lat_tpl[0]), str(lng_lat_tpl[1])]
            if output_lst == -1: continue
            for query, radius in self.query_lst.iteritems():
                rlt_detail = str(self.fetch_nearby_detail_info(lng_lat_tpl, query, radius))
                output_lst.append(str(rlt_detail))
            output_str = "\t".join(output_lst)
            output_file.write("%s\n" % output_str)
            processed_cnt += 1
        output_file.close()

def run_mul_mode():
    """
    以多线程模式运行任务
    """
    app_key = conf.APP_KEY
    retry_limit = conf.RETRY_LIMIT
    query_lst = conf.QUERY_LST
    buff_size = conf.BUFFER_SIZE

    log_dir = conf.LOG_DIR
    input_filename = conf.RESBLOCK_INPUT_FNAME
    output_dir = conf.RESBLOCK_NEARBY_INFO_DIR
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.mkdir(output_dir)
    creator = HouseNearbyInfoCreator(app_key, retry_limit, query_lst, log_dir)
    input_buffer = []
    input_file = open(input_filename, "r")
    thread_lst = []
    thread_num = 0

    for line in input_file:
        input_buffer.append(line.strip())
        if len(input_buffer) == buff_size:
            thread_num += 1
            new_thread = Thread(target=creator.fetch_house_nearby_info_mul, args=(input_buffer, output_dir + "/part_%s.txt" % thread_num))
            new_thread.start()
            input_buffer = []
            thread_lst.append(new_thread)
    if len(input_buffer) > 0:
        thread_num += 1
        new_thread = Thread(target=creator.fetch_house_nearby_info_mul, args=(input_buffer, output_dir + "/part_%s.txt" % thread_num))
        new_thread.start()
        thread_lst.append(new_thread)
    for tmp_thread in thread_lst:
        tmp_thread.join()

def run():
    run_mul_mode()

if __name__ == "__main__":
    run()
