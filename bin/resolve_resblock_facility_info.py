#coding=utf-8
import os
import shutil
import traceback
import sys
sys.path.append("../conf")
import urllib2
import json
import time
import logging
import thread
import conf
import pdb
from threading import Thread

reload(sys)
sys.setdefaultencoding('utf8')

class CommunityIndexCreator:

    def __init__(self, distance_limit, retry_limit, log_dir):
        self.retry_limit = retry_limit
        date_str = time.strftime('%Y%m%d', time.localtime(time.time()))
        self.distance_limit = distance_limit
        logging.basicConfig(level=logging.INFO,\
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',\
                            datefmt='%a, %d %b %Y %H:%M:%S',\
                            filename= log_dir + "/%s.run_feature.log" % date_str, \
                            filemode='w')

    def extract_base_feature(self, input_fname, output_fname, output_detail_fname):
        input_file = open(input_fname, 'r')
        output_file = open(output_fname, 'w')
        detail_file = open(output_detail_fname, 'w')
        for line in input_file:
            try:
                row = line.strip().split("\t")
                resblock_id = row[0]
                resblock_name = row[1]
                super_market = row[4]
                convenience_store = row[5]
                bus = row[6]
                atm = row[7]
                park = row[8]
                western = row[9]
                subway = row[10]
                primary_school = row[11]
                mall = row[12]
                hospital = row[13]
                high_school = row[14]
                chinese = row[15]
                kindergarten = row[16]
                bank = row[17]
                map = {}

                #中餐
                res_lst = eval(chinese)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['中餐']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t1\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['chinese'] = total_site_cnt

                #西餐
                res_lst = eval(western)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['西餐']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t2\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['western'] = total_site_cnt

                #超市
                res_lst = eval(super_market)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['超市']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t3\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['super_market'] = total_site_cnt

                #商场
                res_lst = eval(mall)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['商场']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t4\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['mall'] = total_site_cnt

                #便利店
                res_lst = eval(convenience_store)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['便利店']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t5\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['convenience_store'] = total_site_cnt

                #银行
                res_lst = eval(bank)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['银行']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t6\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['bank'] = total_site_cnt

                #ATM
                res_lst = eval(atm)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['ATM']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t7\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['atm'] = total_site_cnt

                #地铁站
                res_lst = eval(subway)
                subway_map = dict()
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['地铁站']:continue
                        if "address" not in res or (res["address"].find('地铁') == -1 and res["address"].find('线')) == -1:continue
                        l = res['address'].split(';')
                        for name in l : subway_map[name] = 1
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t8\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['subway'] = len(subway_map)

                #公交站
                res_lst = eval(bus)
                bus_map = dict()
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['公交站']:continue
                        if "address" not in res or (res["address"].find("路") == -1 and res["address"].find('线')) == -1:continue
                        l = res["address"].split(';')
                        for name in l : bus_map[name] = 1
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t9\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['bus'] = len(bus_map)

                #公园
                res_lst = eval(park)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['公园']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t10\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['park'] = total_site_cnt

                #医院
                res_lst = eval(hospital)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['医院']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t11\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['hospital'] = total_site_cnt

                #幼儿园
                res_lst = eval(kindergarten)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['幼儿园']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t12\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['kindergarten'] = total_site_cnt

                #小学
                res_lst = eval(primary_school)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['小学']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t13\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['primary_school'] = total_site_cnt

                #中学
                res_lst = eval(high_school)
                total_site_cnt = 0
                for res_iter in res_lst:
                    for res in res_iter:
                        if int(res['detail_info']['distance']) > self.distance_limit['中学']:continue
                        total_site_cnt += 1
                        outpt_str = '{resblock_id}\t{resblock_name}\t{lat}\t{lng}\t14\t{uid}\t{name}\t{address}\t{distance}'.format(
                        resblock_id = resblock_id,
                        resblock_name = resblock_name,
                        lat = row[2],
                        lng = row[3],
                        uid = res['uid'],
                        name = res['name'],
                        address = res['address'],
                        distance = str(res['detail_info']['distance']))
                        print >> detail_file, outpt_str
                map['high_school'] = total_site_cnt

                outpt_str = '{resblock_id}\t{resblock_name}\t{chinese}\t{western}\t{super_market}\t{mall}\t{convenience_store}\t{bank}\t{atm}\t{subway}\t{bus}\t{park}\t{hospital}\t{kindergarten}\t{primary_school}\t{high_school}'.format(
                resblock_id = resblock_id,
                resblock_name = resblock_name,
                chinese = map['chinese'],
                western = map['western'],
                super_market = map['super_market'],
                mall = map['mall'],
                convenience_store = map['convenience_store'],
                bank = map['bank'],
                atm = map['atm'],
                subway = map['subway'],
                bus = map['bus'],
                park = map['park'],
                hospital = map['hospital'],
                kindergarten = map['kindergarten'],
                primary_school = map['primary_school'],
                high_school = map['high_school'])

                print >> output_file, outpt_str

            except Exception, e:
                traceback.print_exc()
                logging.error("processed failed: %s, err: %s" % (line, e))
                continue
        output_file.close()
        detail_file.close()

def run():
    logging.info("Job Start")
    distance_limit = conf.QUERY_LST
    retry_limit = conf.RETRY_LIMIT
    input_dir = conf.COMMUNITY_INPUT_DIR
    log_dir = conf.LOG_DIR
    output_dir = conf.COMMUNITY_OUTPUT_DIR
    output_detail_dir = conf.COMMUNITY_OUTPUT_DETAIL_DIR
    creator = CommunityIndexCreator(distance_limit, retry_limit, log_dir)

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.mkdir(output_dir)
    if os.path.exists(output_detail_dir):
        shutil.rmtree(output_detail_dir)
    os.mkdir(output_detail_dir)
    input_fname_lst = [ input_fname for input_fname in os.listdir(input_dir)]
    thread_lst = []

    for input_fname in input_fname_lst:
        output_fname = output_dir + "/feature_" + input_fname
        output_detail_fname = output_detail_dir + "/feature_detail_" + input_fname
        new_thread = Thread(target = creator.extract_base_feature, \
                        args = (input_dir + "/" + input_fname, output_fname, output_detail_fname))
        new_thread.start()
        thread_lst.append(new_thread)
    for tmp_thread in thread_lst:
        tmp_thread.join()
    logging.info("Job Finished")

if __name__ == "__main__":
    run()
