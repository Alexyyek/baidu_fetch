#!/bin/bash
source "./conf/bash_conf.sh"
RUN_CUR_PATH=`pwd`
############################
##get run_time
############################
echo "RUN_DAY="${RUN_DAY}


###########################
##get param for stat
##########################
CUR_RUN_TIME=${RUN_DAY}

function print_log()
##print log
#param is print string
{
        echo "====================================="
        echo "RUN LOG INFO: ${1}"
        echo "====================================="
}


#################################################
#####获取小区信息
###############################################
function get_resblock
{

    hive -hiveconf pt=${RUN_LAST_DAY}000000 -hiveconf city_code=${city_code} -f ${BIN_PATH}/get_resblock.sql > ${DATA_PATH}/resblock_info/resblock_code_stat.bak
    cp ${DATA_PATH}/resblock_info/resblock_code_stat.bak ${DATA_PATH}/resblock_info/resblock_code_stat
    if [[ $? == 0 ]]
    then
        print_log "get_resblock is success !!!"
    else
        print_log "get_resblock is failed !!!"
        exit 1
   fi
   cd -
}

#################################################
#####爬取百度API
###############################################
function get_resblock_nearby_info
{
    cd ${BIN_PATH}
    python resblock_nearby_info_radius_crawler.py
    if [[ $? == 0 ]]
    then
        print_log "get_resblock_nearby_info is success !!!"
    else
        print_log "get_resblock_nearby_info is failed !!!"
        exit 1
    fi
    cd -
}

#################################################
#####补全漏爬小区
###############################################
function get_miss_resblock_nearby_info
{
    mkdir ${DATA_PATH}/tmp
    cat ${DATA_PATH}/resblock_nearby_info/part* > ${DATA_PATH}/tmp/part
    cd ${BIN_PATH}
    python patch_miss_resblock.py
    python resblock_nearby_info_radius_crawler.py
    cat ${DATA_PATH}/resblock_nearby_info/part* >> ${DATA_PATH}/tmp/part
    rm ${DATA_PATH}/resblock_nearby_info/*
    split -a 2 -d -l 500 ${DATA_PATH}/tmp/part ${DATA_PATH}/resblock_nearby_info/part
    find ${DATA_PATH}/resblock_nearby_info -type f | xargs -i mv {} {}.txt
    rm -r ${DATA_PATH}/tmp
    if [[ $? == 0 ]]
    then
        print_log "get_miss_resblock_nearby_info is success !!!"
    else
        print_log "get_miss_resblock_nearby_info is failed !!!"
        exit 1
   fi
   cd -
}

#################################################
#####解析爬取信息
###############################################
function resolve_resblock_facility_info
{
    cd ${BIN_PATH}
    python resolve_resblock_facility_info.py
    if [[ $? == 0 ]]
    then
        print_log "resolve_resblock_facility_info is success !!!"
    else
        print_log "resolve_resblock_facility_info is failed !!!"
        exit 1
    fi
    cd -
}

get_resblock
get_resblock_nearby_info
get_miss_resblock_nearby_info
resolve_resblock_facility_info
