#coding=utf-8
import sys
import os
import shutil
sys.path.append("../conf")
import traceback
import conf
import pdb

def extract_facility_uid():
    detail_dir= conf.COMMUNITY_OUTPUT_DETAIL_DIR
    output_dir = conf.COMMINITY_FACILITY_UID_DIR
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.mkdir(output_dir)

    restaurant_lst = []
    park_lst = []
    mall_lst = []

    detail_dir_lst = [ input_fname for input_fname in os.listdir(detail_dir) if input_fname.endswith("txt")]
    for input_fname in detail_dir_lst:
        for line in open(detail_dir + '/' + input_fname,'r'):
            row = line.strip().split('\t')
            if row[4] == '5':
                if row[5] in restaurant_lst:
                    continue
                else:
                    restaurant_lst.append(row[5])
            elif row[4] == '8':
                if row[5] in park_lst:
                    continue
                else:
                    park_lst.append(row[5])
            elif row[4] == '9':
                if row[5] in mall_lst:
                    continue
                else:
                    mall_lst.append(row[5])

    restaurant_opt_fname = output_dir + '/restaurant.txt'
    restaurant_file = open(restaurant_opt_fname,'w')
    for row in restaurant_lst:
        restaurant_file.write(row + "\n")
    restaurant_file.close()

    park_opt_fname = output_dir + '/park.txt'
    park_file = open(park_opt_fname,'w')
    for row in park_lst:
        park_file.write(row + "\n")
    park_file.close()

    mall_opt_fname = output_dir + '/mall.txt'
    mall_file = open(mall_opt_fname,'w')
    for row in mall_lst:
        mall_file.write(row + "\n")
    mall_file.close()

if __name__ == "__main__":
    extract_facility_uid()
