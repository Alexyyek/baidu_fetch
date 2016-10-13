#coding:utf-8
RUN_PATH = '/home/work/yangyekang/resblock/fetch'
BIN_PATH = RUN_PATH+'/bin'
DAT_PATH = RUN_PATH+'/data'

#FILE
RESBLOCK_NEARBY_INFO_DIR = DAT_PATH + '/resblock_nearby_info'
RESBLOCK_INPUT_FNAME = DAT_PATH + '/resblock_info/resblock_code_stat'
RESBLOCK_INPUT_FNAME_BAK = DAT_PATH + '/resblock_info/resblock_code_stat.bak'

COMMINITY_FACILITY_UID_DIR = DAT_PATH + '/uid'
COMMUNITY_OUTPUT_DIR = DAT_PATH + '/comm_feature'
COMMUNITY_OUTPUT_DETAIL_DIR = DAT_PATH + '/detail'
COMMUNITY_OUTPT_FACILITY_DIR = DAT_PATH + '/facility'
COMMUNITY_INPUT_DIR = DAT_PATH + '/resblock_nearby_info'

RETRY_LIMIT = 3
BUFFER_SIZE = 600
LOG_DIR = "../data/log"

APP_KEY = [
    "eGXiWVPmDIebmytRPquYo2Rv4P2PRVLd",
    "nuOgLRnRoGHMQji0zdpKoSOOBwaK23Bp",
    "5lEdIPHgTkbNG5UkIsPPxpFFDYdNAxM0",
    "ps3rlr8GddICDNDrv0kKeNWegGGB5qAh",
    "VGywmInIilqeMbyyArKBBaP1rTzdlHjg",
    "VDiQBxlQzanxYpPEBfqo4ryEptLvsz0b"
    "yoBIFMZGqaXqvAyCpL7M5jj9",
    ]
QUERY_LST = {
    "中餐":1000,
    "西餐":1000,
    "超市":1000,
    "商场":1000,
    "便利店":1000,
    "银行":1000,
    "ATM":1000,
    "地铁站":1000,
    "公交站":1000,
    "公园":3000,
    "医院":5000,
    "幼儿园":3000,
    "小学":3000,
    "中学":3000
    }
