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

    def __init__(self, app_key, retry_limit, log_dir):
        self.app_key = app_key
        self.retry_limit = retry_limit
        date_str = time.strftime('%Y%m%d', time.localtime(time.time()))
        logging.basicConfig(level=logging.INFO,\
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',\
                            datefmt='%a, %d %b %Y %H:%M:%S',\
                            filename= log_dir + "/%s.run_feature.log" % date_str, \
                            filemode='w')

    def extract_base_feature(self, input_fname, output_facility_dir):
        input_file = open(input_fname, 'r')
        for line in input_file:
            res = str(self.fetch_place_detail(line.strip()))
            if res == -1: continue
            if os.path.basename(input_fname) == "restaurant.txt":
                self.extract_place_restaurant(res,output_facility_dir + "/" + os.path.basename(input_fname))
            elif os.path.basename(input_fname) == "park.txt":
                self.extract_place_park(res, output_facility_dir + "/" + os.path.basename(input_fname))
            elif os.path.basename(input_fname) == "mall.txt":
                self.extract_place_mall(res, output_facility_dir + "/" + os.path.basename(input_fname))
        logging.info("Job Finished")

    def extract_place_mall(self, input, output_fname):
        output_file = open(output_fname,'a')
        try:
            res = eval(input)
            name = res.has_key("name") and res["name"] or ""
            address = res.has_key("address") and res["address"] or ""
            telephone = res.has_key("telephone") and res["telephone"] or ""
            uid = res.has_key("uid") and res["uid"] or ""
            detail_info = res["detail_info"]
            tag = detail_info.has_key("tag") and detail_info["tag"] or ""
            price = detail_info.has_key("price") and detail_info["price"] or ""
            overall_rating = detail_info.has_key("overall_rating") and detail_info["overall_rating"] or ""
            enviroment_rating = detail_info.has_key("environment_rating") and detail_info["environment_rating"] or ""
            service_rating = detail_info.has_key("service_rating") and detail_info["service_rating"] or ""
            comment_num = detail_info.has_key("comment_num") and detail_info["comment_num"] or ""
            shop_hours = detail_info.has_key("shop_hours") and detail_info["shop_hours"] or ""
            description = detail_info.has_key("description") and detail_info["description"] or ""
            if detail_info.has_key("di_review_keyword"):
                di_review_keyword = detail_info["di_review_keyword"]
                for review in di_review_keyword:
                    keyword = review.has_key("keyword") and review["keyword"] or ""
                    keyword_category_name = review.has_key("keyword_category_name") and review["keyword_category_name"] or ""
                    keyword_tag = review.has_key("keyword_tag") and review["keyword_tag"] or ""
                    keyword_desc = review.has_key("keyword_desc") and review["keyword_desc"] or ""
                    keyword_num = review.has_key("keyword_num") and review["keyword_num"] or ""
                    print >> output_file, name + '\t' + address + '\t' + str(telephone) + '\t' + str(uid) + '\t' + tag + '\t' + str(price) + '\t' + str(overall_rating) + '\t' + str(enviroment_rating) + '\t' + str(service_rating) + '\t' + str(comment_num) + '\t' + str(shop_hours) + '\t' + description + '\t' +  keyword + '\t' + keyword_category_name + '\t' + keyword_desc + '\t' + keyword_tag + '\t' + str(keyword_num)
            else:
                print >> output_file, name + '\t' + address + '\t' + str(telephone) + '\t' + str(uid) + '\t' + tag + '\t' + str(price) + '\t' + str(overall_rating) + '\t' + str(enviroment_rating) + '\t' + str(service_rating) + '\t' + str(comment_num) + '\t' + str(shop_hours) + '\t' + description + '\t' + '' + '\t' + '' + '\t' + '' + '\t' + '' + '\t' + ''
        except Exception, e:
            traceback.print_exc()
            logging.error("processed failed: %s, err: %s" %(input, e))
        output_file.close()

    def extract_place_park(self, input, output_fname):
        output_file = open(output_fname,'a')
        try:
            res = eval(input)
            name = res.has_key("name") and res["name"] or ""
            address = res.has_key("address") and res["address"] or ""
            uid = res.has_key("uid") and res["uid"] or ""
            detail_info = res["detail_info"]
            tag = detail_info.has_key("tag") and detail_info["tag"] or ""
            overall_rating = detail_info.has_key("overall_rating") and detail_info["overall_rating"] or ""
            comment_num = detail_info.has_key("comment_num") and detail_info["comment_num"] or ""
            scope_type = detail_info.has_key("scope_type") and detail_info["scope_type"] or ""
            description = detail_info.has_key("description") and detail_info["description"] or ""
            if detail_info.has_key("di_review_keyword"):
                di_review_keyword = detail_info["di_review_keyword"]
                for review in di_review_keyword:
                    keyword = review.has_key("keyword") and review["keyword"] or ""
                    keyword_category_name = review.has_key("keyword_category_name") and review["keyword_category_name"] or ""
                    keyword_tag = review.has_key("keyword_tag") and review["keyword_tag"] or ""
                    keyword_desc = review.has_key("keyword_desc") and review["keyword_desc"] or ""
                    keyword_num = review.has_key("keyword_num") and review["keyword_num"] or ""
                    print >> output_file, name + '\t' + address + '\t' + str(uid) + '\t' + tag + '\t' + str(overall_rating) + '\t' + str(comment_num) + '\t' + scope_type + '\t' + description + '\t' +  keyword + '\t' + keyword_category_name + '\t' + keyword_desc + '\t' + keyword_tag + '\t' + str(keyword_num)
            else:
                print >> output_file, name + '\t' + address + '\t' + str(uid) + '\t' + tag + '\t' + str(overall_rating) + '\t' + str(comment_num) + '\t' + scope_type + '\t' + description + '\t' + '' + '\t' + '' + '\t' + '' + '\t' + '' + '\t' + ''
        except Exception, e:
            traceback.print_exc()
            logging.error("processed failed: %s, err: %s" %(input, e))
        output_file.close()


    def extract_place_restaurant(self, input, output_fname):
        output_file = open(output_fname,'a')
        try:
            res = eval(input)
            name = res.has_key("name") and res["name"] or ""
            address = res.has_key("address") and res["address"] or ""
            telephone = res.has_key("telephone") and res["telephone"] or ""
            uid = res.has_key("uid") and res["uid"] or ""
            detail_info = res["detail_info"]
            tag = detail_info.has_key("tag") and detail_info["tag"] or ""
            price = detail_info.has_key("price") and detail_info["price"] or ""
            overall_rating = detail_info.has_key("overall_rating") and detail_info["overall_rating"] or ""
            taste_rating = detail_info.has_key("taste_rating") and detail_info["taste_rating"] or ""
            enviroment_rating = detail_info.has_key("environment_rating") and detail_info["environment_rating"] or ""
            service_rating = detail_info.has_key("service_rating") and detail_info["service_rating"] or ""
            comment_num = detail_info.has_key("comment_num") and detail_info["comment_num"] or ""
            shop_hours = detail_info.has_key("shop_hours") and detail_info["shop_hours"] or ""
            atmosphere = detail_info.has_key("atmosphere") and detail_info["atmosphere"] or ""
            featured_service = detail_info.has_key("featured_service") and detail_info["featured_service"] or ""
            recommendation = detail_info.has_key("recommendation") and detail_info["recommendation"] or ""
            description = detail_info.has_key("description") and detail_info["description"] or ""
            if detail_info.has_key("di_review_keyword"):
                di_review_keyword = detail_info["di_review_keyword"]
                for review in di_review_keyword:
                    keyword = review.has_key("keyword") and review["keyword"] or ""
                    keyword_category_name = review.has_key("keyword_category_name") and review["keyword_category_name"] or ""
                    keyword_tag = review.has_key("keyword_tag") and review["keyword_tag"] or ""
                    keyword_desc = review.has_key("keyword_desc") and review["keyword_desc"] or ""
                    keyword_num = review.has_key("keyword_num") and review["keyword_num"] or ""
                    print >> output_file, name + '\t' + address + '\t' + str(telephone) + '\t' + str(uid) + '\t' + tag + '\t' + str(price) + '\t' + str(overall_rating) + '\t' + str(taste_rating) + '\t' + str(enviroment_rating) + '\t' + str(service_rating) + '\t' + str(comment_num) + '\t' + str(shop_hours) + '\t' + atmosphere + '\t' + featured_service + '\t' + recommendation + '\t' + description + '\t' +  keyword + '\t' + keyword_category_name + '\t' + keyword_desc + '\t' + keyword_tag + '\t' + str(keyword_num)
            else:
                print >> output_file, name + '\t' + address + '\t' + str(telephone) + '\t' + str(uid) + '\t' + tag + '\t' + str(price) + '\t' + str(overall_rating) + '\t' + str(taste_rating) + '\t' + str(enviroment_rating) + '\t' + str(service_rating) + '\t' + str(comment_num) + '\t' + str(shop_hours) + '\t' + atmosphere + '\t' + featured_service + '\t' + recommendation + '\t' + description + '\t' + '' + '\t' + '' + '\t' + '' + '\t' + '' + '\t' + ''
        except Exception, e:
            traceback.print_exc()
            logging.error("processed failed: %s, err: %s" %(input, e))
        output_file.close()

    def fetch_place_detail(self, uid):
        """
        通过百度API获取指定POI点的详细信息，如好评，评价等
        """
        key = self.app_key
        key_index = 0
        content = ""
        api_url = "http://api.map.baidu.com/place/v2/detail?uid=%s&output=json&scope=2&ak=%s" % (uid,key[key_index])

        while(key_index < len(key)):
            retry_cnt = 0
            while(retry_cnt <= self.retry_limit):
                try:
                    content = urllib2.urlopen(api_url, timeout=3).read()
                    isValid = eval(content)
                    if isValid["status"] != 0:
                        retry_cnt += 1
                        continue
                    rlt_dic = json.loads(content)
                    rlt_detail = rlt_dic["result"]
                    return rlt_detail
                except Exception,e:
                    print api_url, content, e
                    logging.error("request: %s, response: %s, err: %s" %(api_url, content, e))
                    retry_cnt += 1
                    continue
            key_index += 1
            if key_index < len(key):
                logging.info("inValid key :%s, retry_time : %s, Changing key : %s" % (key[key_index -1], retry_cnt, key[key_index]))
        return -1

def run():
    logging.info("Job Start")
    app_key = conf.APP_KEY
    retry_limit = conf.RETRY_LIMIT
    input_dir = conf.COMMINITY_FACILITY_UID_DIR
    log_dir = conf.LOG_DIR
    output_facility_dir = conf.COMMUNITY_OUTPT_FACILITY_DIR
    creator = CommunityIndexCreator(app_key, retry_limit, log_dir)

    if os.path.exists(output_facility_dir):
        shutil.rmtree(output_facility_dir)
    os.mkdir(output_facility_dir)
    input_fname_lst = [ input_fname for input_fname in os.listdir(input_dir) if input_fname.endswith("txt")]
    thread_lst = []

    for input_fname in input_fname_lst:
        new_thread = Thread(target = creator.extract_base_feature, \
                        args = (input_dir + "/" + input_fname, output_facility_dir))
        new_thread.start()
        thread_lst.append(new_thread)
    for tmp_thread in thread_lst:
        tmp_thread.join()
    logging.info("Job Finished")

if __name__ == "__main__":
    run()
