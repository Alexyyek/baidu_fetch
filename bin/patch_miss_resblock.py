#coding=utf-8
import sys
import os
sys.path.append("../conf")
import traceback
import conf
import pdb

def get_miss_resblock():
    crawler_dir = conf.RESBLOCK_NEARBY_INFO_DIR
    dataset_fname = conf.RESBLOCK_INPUT_FNAME_BAK
    output_dir = conf.RESBLOCK_INPUT_FNAME

    crawler_lst = []

    for root, dirs, files in os.walk(crawler_dir):
        for file in files:
            file = crawler_dir + '/' + file
            input = open(file,'r')
            for line in input:
                try:
                    row = line.strip().split("\t")
                    resblock_name = row[1]
                    crawler_lst.append(resblock_name)
                except Exception, e:
                    traceback.print_exc()
                    continue

    output_file = open(output_dir,'w')
    dataset_file = open(dataset_fname, 'r')
    for line in dataset_file:
        row = line.strip().split("\t")
        resblock_name = row[1]+ row[3] + row[5] + row[7]
        if resblock_name not in crawler_lst:
            output_file.write(line)
    dataset_file.close()

if __name__ == "__main__":
    get_miss_resblock()
