import time
import urllib
import random
import sys
# import pytesseract
import requests
import os
import string
import datetime
import base64
import queue
import threading
import re
import logging


from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from DB_CONNECTION import connection

# from PIL import Image
# from io import BytesIO

# import numpy as np, matplotlib.pyplot as plt
""" 
貨到時SQLPASS UPDATE STATUS
Load data from sqlpaas
"""

"""
ELK data format
doc = {
    'author': 'kimchy',
    'text': 'Elasticsearch: cool. bonsai cool.',
    'timestamp': datetime.now(),
}
res = es.index(index="test-index", doc_type='tweet', id=1, body=doc)

print(res['created'])

res = es.get(index="test-index", doc_type='tweet', id=1)
print(res['_source'])

es.indices.refresh(index="test-index")
"""
###
"""
doc = {
    'ORD_NUM': '128073154',
    'PACKAGE_NO': '760053488326',
    'PACKAGE_STATUS': {xxx},
    'DELIVERY': {'START_TIME':xxx, 'END_TIME':xxx} 
    'FLAG': '0',
    'UPDATE_TIME': 'xxx'
}
"""


class log():

    __FORMAT = '%(asctime)s - %(levelname)s - %(name)-12s - %(message)s'

    @classmethod
    def WRITE(cls, name, string):

        logging.basicConfig(format=cls.__FORMAT, level=logging.WARNING, filename='logistic.log')
        logger = logging.getLogger(name)
        logger.warning(string)


class MyThread(threading.Thread):

    def __init__(self, func):

        super(MyThread, self).__init__()  ### 調用父類的結構函數
        self.func = func  ### 傳入線程函數邏輯
        self._stop_event = threading.Event() ### 線程停止的方法

        self.threadLock = threading.Lock()

    def run(self):

        self.threadLock.acquire()
        self.func()
        self.threadLock.release()

    def stop(self):

        self._stop_event.set()

    def stopped(self):

        return self._stop_event.is_set()

class request(object):

    @classmethod
    def get_page_utf8(cls, url, parameters=None):

        if parameters:
            encode_parameters = urllib.parse.urlencode(parameters).encode('utf-8')
        else:
            encode_parameters = None
        time.sleep(0.33)
        request = urllib.request.Request(url)

        ####隨機header挑選####
        foo = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36',
            'Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; da-dk) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2722.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'
        ]
        headers = str(random.choice(foo))
        ####隨機header挑選####

        request.add_header('User-Agent', headers)
        time.sleep(0.42)
        response = urllib.request.urlopen(request, data=encode_parameters, timeout=180)
        html = BeautifulSoup(response.read().decode('utf-8'), 'lxml')
        response.close()

        return html

    @classmethod
    def get_page_big5(cls, url, parameters=None):

        if parameters:
            encode_parameters = urllib.parse.urlencode(parameters).encode('utf-8')
        else:
            encode_parameters = None
        time.sleep(0.33)
        request = urllib.request.Request(url)

        ####隨機header挑選####
        foo = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36',
            'Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; da-dk) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2722.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'
        ]
        headers = str(random.choice(foo))
        ####隨機header挑選####

        request.add_header('User-Agent', headers)
        time.sleep(0.42)
        response = urllib.request.urlopen(request, data=encode_parameters, timeout=180)
        html = BeautifulSoup(response.read().decode('big5'), 'lxml')
        response.close()

        return html


###新竹物流### 貨件已由鹿港營業所送達。貨物件數共1件
class hct(request, MyThread):

    SHARE_Q = queue.Queue()
    _WORKER_THREAD_NUM = 5

    @staticmethod
    def b64_encode(string):

        result = base64.b64encode(str(string).encode('utf-8')).decode('utf-8')

        return result

    @classmethod
    def parse_hct(cls, item):
        # pack_no = str(item)
        pack_no = str(item[1])
        ord_num = item[0]

        url = 'https://www.hct.com.tw/SearchGoods.aspx?no='

        package_no_b64 = hct.b64_encode(pack_no)

        attempts = 0
        while attempts < 5:
            try:
                result = request.get_page_utf8(url + package_no_b64)

                arrival = 0
                # no_status = 1 ### default 1, if non arrive then 0
                hct_list = list()

                # for i in result.find_all('span', {'id': 'lbl_stats'}):
                #     if '查無此配送資料' in i.text:
                #         no_status = 0
                #
                # if no_status == 0:
                #     update(0, pack_no)
                #     break

                if result.find_all('td', {'class': 'pad'}):
                    for i in result.find_all('td', {'class': 'pad'}):
                        text = i.text
                        hct_list.append(text)
                        ### verify package arrival
                        if '送達' in i.text:
                            arrival = 1
                        ### verify package arrival
                        if text is None:
                            log.WRITE('新竹物流', '{}, HTML格式可能改變'.format(pack_no))
                else:
                    update(0, pack_no)
                    break

                now = datetime.datetime.today().strftime("%Y-%m%d-%H:%M:%S")

                body = [{'作業時間': hct_list[i],
                        '貨物追蹤': hct_list[i+1]
                        }for i in range(0, len(hct_list), 2)]

                # hct_dict = {hct_list[i]: hct_list[i + 1] for i in range(0, len(hct_list), 2)}

                doc = {
                    'ORD_NUM': ord_num,
                    'PACKAGE_NO': pack_no,
                    'PACKAGE_STATUS': body,
                    'FLAG': arrival,
                    'UPDATE_TIME': now
                }

                connection.ELK.handle_ES('新竹物流', 'HCT', doc, pack_no)

                if arrival == 1:
                    update(1, pack_no)
                else:
                    update(0, pack_no)

                break

            except Exception as e:
                attempts += 1
                if attempts == 4:
                    log.WRITE('新竹物流', '{}, {}'.format(pack_no, e))


    @classmethod
    def worker(cls):
        while True:
            if not cls.SHARE_Q.empty():
                item = cls.SHARE_Q.get()
                cls.parse_hct(item)
                cls.SHARE_Q.task_done()
            else:
                break

    @classmethod
    def hct_main(cls):
        # a = [8559603931, 8559603743, 8559603905, 8818200565]
        sql_stat = ('''select [ORD_NUM], [PACKAGE_NO] from [dbo].[LOGISTIC_STATUS]
                       where [SCT_DESC] = '新竹貨運' and [PACKAGE_STATUS] = 0 ''')
        result = connection.db('AZURE').do_query(sql_stat)
        threads = []
        ### 向隊列中放入任務, 真正使用時, 應該設置為可持續的放入任務
        for task in result:
            cls.SHARE_Q.put(task)
        ### 開啟_WORKER_THREAD_NUM個線程
        for i in range(cls._WORKER_THREAD_NUM):
            thread = MyThread(cls.worker)
            time.sleep(0.27)
            # thread.setDaemon(True)
            thread.start() ### 線程開始處理任務
            thread.join()
        # for thread in threads:
        #     thread.join()
        ### 等待所有任務完成
        cls.SHARE_Q.join()



###黑貓### 順利送達
class t_cat(request, MyThread):

    SHARE_Q = queue.Queue()
    _WORKER_THREAD_NUM = 5

    @classmethod
    def parse_tcat(cls, item):
        # pack_no = str(item)
        pack_no = str(item[1])
        ord_num = item[0]

        url = "http://www.t-cat.com.tw/Inquire/TraceDetail.aspx?BillID={}&ReturnUrl=Trace.aspx".format(str(pack_no))

        ###test package_no : 905224497856, 905219336964, 905224497590
        # payload = {'__EVENTTARGET': 'ctl00$ContentPlaceHolder1$btnSend',
        #            '__VIEWSTATE': '/wEPDwULLTE2ODAyMTAzNDBkZHngR2yLNdcoB1YXtf+bAIxi/AHF',
        #            '__VIEWSTATEGENERATOR': '9A093EFF',
        #            '__EVENTVALIDATION': '/wEWDALXz8K8AwKUhrKJAQL5nJT0BgLes/beDALDytjJAgKo4bq0CAKN+JyfDgLyjv+JBAKHub7IDALsz6CzAgKUhvK7DAK97Mp+etzK3cOKerX3pzYyBL/kZYAJxkM=',
        #            'q': '站內搜尋',
        #            'cx': '005475758396817196247:vpg-mgvhr44',
        #            'cof': 'FORID:11',
        #            'ie': 'UTF-8',
        #            'ctl00$ContentPlaceHolder1$txtQuery1': str(pack_no),
        #            'ctl00$ContentPlaceHolder1$txtQuery2': '',
        #            'ctl00$ContentPlaceHolder1$txtQuery3': '',
        #            'ctl00$ContentPlaceHolder1$txtQuery4': '',
        #            'ctl00$ContentPlaceHolder1$txtQuery5': '',
        #            'ctl00$ContentPlaceHolder1$txtQuery6': '',
        #            'ctl00$ContentPlaceHolder1$txtQuery7': '',
        #            'ctl00$ContentPlaceHolder1$txtQuery8': '',
        #            'ctl00$ContentPlaceHolder1$txtQuery9': '',
        #            'ctl00$ContentPlaceHolder1$txtQuery10': ''}
        #
        # headers = {
        #     'origin': "http://www.t-cat.com.tw",
        #     'upgrade-insecure-requests': "1",
        #     'user-agent': "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36",
        #     'content-type': "application/x-www-form-urlencoded",
        #     'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        #     'referer': "http://www.t-cat.com.tw/Inquire/trace.aspx",
        #     'accept-encoding': "gzip, deflate",
        #     'accept-language': "zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4",
        #     'cookie': "ASP.NET_SessionId=scc5f2jtods2jfjv3v1ocl55; citrix_ns_id=aiBnezMSMEtxItV5I4rX03tU3RIA010; __utmt=1; __utma=8454064.936897360.1494375638.1494375638.1494375638.1; __utmb=8454064.1.10.1494375638; __utmc=8454064; __utmz=8454064.1494375638.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)",
        #     'cache-control': "no-cache"
        # }

        attempts = 0
        while attempts < 5:
            try:
                result = request.get_page_utf8(url)

                arrival = 0

                tcat_list = list()

                if result.find_all('td', {'class':'style1'}):
                    for i in result.find_all('td', {'class':'style1'}):
                        text = i.text.replace(' ', '')
                        tcat_list.append(text)
                        if '順利送達' in text:
                            arrival = 1
                        if text is None:
                            log.WRITE('黑貓宅急便', '{}, HTML格式可能改變'.format(pack_no))
                else:
                    update(0, pack_no)
                    break

                body = [{'目前狀態': tcat_list[i],
                         '資料登入時間': tcat_list[i+1][:-5] + ' ' + tcat_list[i+1][-5:],
                         '負責營業所': tcat_list[i+2]} for i in range(0, len(tcat_list), 3)]

                now = datetime.datetime.today().strftime("%Y-%m%d-%H:%M:%S")

                doc = {
                    'ORD_NUM': ord_num,
                    'PACKAGE_NO': pack_no,
                    'PACKAGE_STATUS': body,
                    'FLAG': arrival,
                    'UPDATE_TIME': now,
                }

                connection.ELK.handle_ES('黑貓宅急便', 'TCAT', doc, pack_no)

                if arrival == 1:
                    update(1, pack_no)
                else:
                    update(0, pack_no)

                break

            except Exception as e:
                attempts += 1
                if attempts == 4:
                    log.WRITE('黑貓宅急便', '{}, {}'.format(pack_no, e))

    @classmethod
    def worker(cls):
        while True:
            if not cls.SHARE_Q.empty():
                item = cls.SHARE_Q.get()
                cls.parse_tcat(item)
                cls.SHARE_Q.task_done()
            else:
                break

    @classmethod
    def tcat_main(cls):
        # a = [905231736760]
        sql_stat = ('''select [ORD_NUM], [PACKAGE_NO] from [dbo].[LOGISTIC_STATUS]
                       where [SCT_DESC] = '統一速達(黑貓宅急便)' and [PACKAGE_STATUS] = 0 ''')
        result = connection.db('AZURE').do_query(sql_stat)
        threads = []

        for task in result:
            cls.SHARE_Q.put(task)

        for i in range(cls._WORKER_THREAD_NUM):
            thread = MyThread(cls.worker)
            time.sleep(0.27)
            # thread.setDaemon(True)
            thread.start()
            # threads.append(thread)
        # for thread in threads:
            thread.join()
        cls.SHARE_Q.join()

###郵局### 投遞成功
class pstmail(MyThread):

    SHARE_Q = queue.Queue()
    _WORKER_THREAD_NUM = 5

    class pstmail_data:

        @classmethod
        def random_uuid(cls):

            pst_url = "http://postserv.post.gov.tw/pstmail/jcaptcha?uuid="
            digits = string.digits
            ascii_low = string.ascii_lowercase
            ### sample : af37b7e2-a4af-4e3d-836b-ca8b8f11c530
            # uuid_1 = ''.join(random.choice(digits + ascii_low) for _ in range(8)) + '-'
            # uuid_2 = ''.join(random.choice(digits + ascii_low) for _ in range(4)) + '-'
            # uuid_3 = ''.join(random.choice(digits + ascii_low) for _ in range(4)) + '-'
            # uuid_4 = ''.join(random.choice(digits + ascii_low) for _ in range(4)) + '-'
            uuid_5 = ''.join(random.choice(digits + ascii_low) for _ in range(12))

            uuid = 'af37b7e2-a4af-4e3d-836b-' + uuid_5
            pst_url_uuid = pst_url + uuid

            return (pst_url_uuid, uuid)

        @classmethod
        def pic_handle(cls):

            ### random create uuid and url
            data = cls.random_uuid()
            ### random create uuid and url
            pst_url = data[0]
            uuid = data[1]

            pic_path = 'C:\\Users\\kevin_huang\\Logistic_Parse\\pic\\'
            pic_name = str(uuid) + '.png'

            def pic_request():

                def pic_save(data, fn):

                    sizes = np.shape(data)
                    height = float(sizes[0])
                    width = float(sizes[1])
                    fig = plt.figure()
                    fig.set_size_inches(width / height, 1, forward=False)
                    ax = plt.Axes(fig, [0., 0., 1., 1.])
                    ax.set_axis_off()
                    fig.add_axes(ax)
                    ax.imshow(data)
                    plt.savefig(fn, dpi=300)
                    plt.close()

                response = requests.get(pst_url)
                img = Image.open(BytesIO(response.content))
                # img.save('C:\\Users\\kevin_huang\\Logistic_Parse\\pic\\haha.jpg', quality=95)
                THRESHOLD_VALUE = 40
                # img = Image.open("C:\\Users\\kevin_huang\\Logistic_Parse\\pic\\haha.jpg")
                img = img.convert("L")
                img = img.resize((150, 50), Image.ANTIALIAS)
                imgData = np.asarray(img)
                thresholdedData = (imgData > THRESHOLD_VALUE) * 5000000

                pic_save(thresholdedData, pic_path + pic_name)

            def pic_OCR():

                # image_path = 'C:\\Users\\kevin_huang\\Logistic_Parse\\pic\\'

                clean_result = str()

                while len(clean_result) != 4:
                    ### exec pic_request ###
                    pic_request()
                    ### exec pic_request ###
                    # png_files = [f for f in os.listdir(pic_path) if f.endswith('.png')][0]
                    image_png = Image.open(os.path.join(pic_path, pic_name))
                    dirty_result = pytesseract.image_to_string(image_png, config='digits')
                    clean_result = dirty_result.replace(' ','').replace('.','').replace('-','')
                    # os.remove(pic_path + pic_name)

                return clean_result

            return uuid, pic_OCR()



    @classmethod
    def parse_pst(cls, item):
        pack_no = str(item)
        # pack_no = str(item[1])
        # ord_num = item[0]

        url = "http://postserv.post.gov.tw/pstmail/EsoafDispatcher"

        # data = pstmail.pstmail_data.pic_handle()
        # uuid = data[0]
        # captcha = data[1]

        ### 5 個 mail_info 參數，UUID，CAPCHA
        # payload = """{"header":{"InputVOClass":"com.systex.jbranch.app.server.post.vo.EB500100InputVO",
        #                         "TxnCode":"EB500100","BizCode":"query","StampTime":true,"SupvPwd":"",
        #                         "TXN_DATA":{},"SupvID":"","CustID":"","REQUEST_ID":"","ClientTransaction":true,
        #                         "DevMode":false,"SectionID":"esoaf"},
        #               "body":{"MAILNO1":""" + "'" + str(pack_no) + "'" + """,
        #                       "MAILNO2":"",
        #                       "MAILNO3":"",
        #                       "MAILNO4":"",
        #                       "MAILNO5":"",
        #                       "uuid":""" + "'" + str(uuid) + "'" + """,
        #                       "captcha":""" + "'" + str(captcha) + "'" + """,
        #                       "pageCount":10}}"""

        payload = """{"header":
                        {"InputVOClass":"com.systex.jbranch.app.server.post.vo.EB500100InputVO",
                         "TxnCode":"EB500100","BizCode":"query2","StampTime":true,"SupvPwd":"",
                         "TXN_DATA":{},"SupvID":"","CustID":"","REQUEST_ID":"","ClientTransaction":true,
                         "DevMode":false,"SectionID":"esoaf"},
                      "body":{"MAILNO":""" + "'" + pack_no + "'" + ""","pageCount":10}}"""
        headers = {
            'accept': "application/json, text/plain, */*",
            'origin': "http://postserv.post.gov.tw",
            'user-agent': "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36",
            'content-type': "application/json;charset=UTF-8",
            'referer': "http://postserv.post.gov.tw/pstmail/main_mail.html",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4",
            'cache-control': "no-cache"
        }

        attempts = 0
        while attempts < 5:
            try:
                response = requests.request("POST", url, data=payload, headers=headers)
                response_text = response.text.replace('[', '').replace(']', '').replace(' ','').replace('\u3000', '')

                if '無此資料' in response_text or not response_text:
                    update(0, pack_no)
                    break

                pattern = 'DATIME\":\".*?\"|STATUS\":\".*?\"|BRHNC\":\".*?\"'
                result = re.findall(pattern, response_text)[1:]

                ### verify 'DATIME":""' in result or not ###
                if 'DATIME":""' in result:
                    result.remove('DATIME":""')
                ### verify 'DATIME":""' in result or not ###

                clean_1 = list()
                arrival = 0

                if result:
                    for i in result:
                        text = i.replace('"', '').replace('BRHNC', '處理單位').replace('DATIME', '處理日期時間').replace('STATUS', '目前狀態')
                        clean_1.append(text)
                        if '投遞成功' in text:
                            arrival = 1
                    # clean_1 = [i.replace('"', '').replace('BRHNC', '處理單位').replace('DATIME', '處理日期時間').replace('STATUS', '目前狀態') for i in result]
                    clean_2 = [[clean_1[i], clean_1[i + 1], clean_1[i + 2]] for i in range(0, len(clean_1), 3)]

                    body = [{clean_2[i][0].split(':')[0]: clean_2[i][0].split(':')[1],
                             clean_2[i][1].split(':')[0]: clean_2[i][1].split(':')[1],
                             clean_2[i][2].split(':')[0]: clean_2[i][2].split(':')[1]} for i in range(0, len(clean_2))]

                    now = datetime.datetime.today().strftime("%Y-%m%d-%H:%M:%S")

                    doc = {
                        'ORD_NUM': 'test',
                        'PACKAGE_NO': pack_no,
                        'PACKAGE_STATUS': body,
                        'FLAG': arrival,
                        'UPDATE_TIME': now,
                    }
                    connection.ELK.handle_ES('郵局', 'PSTMAIL', doc, pack_no)

                    if arrival == 1:
                        update(1, pack_no)
                    else:
                        update(0, pack_no)

                    break

                else:
                    log.WRITE('郵局', '{}, JSON格式可能改變'.format(pack_no))

            except Exception as e:
                attempts += 1
                if attempts == 4:
                    log.WRITE('郵局', '{}, {}'.format(pack_no, e))

    @classmethod
    def worker(cls):
        while True:
            if not cls.SHARE_Q.empty():
                print(cls.SHARE_Q.empty())
                item = cls.SHARE_Q.get()
                cls.parse_pst(item)
                cls.SHARE_Q.task_done()
            else:
                break

    @classmethod
    def pstmail_main(cls):

        # a = [97129760003210238002, 77448222027818]
        sql_stat = ('''select [ORD_NUM], [PACKAGE_NO] from [dbo].[LOGISTIC_STATUS]
                       where [SCT_DESC] = '郵局' and [PACKAGE_STATUS] = 0 ''')
        result = connection.db('AZURE').do_query(sql_stat)

        threads = []

        for task in result:
            cls.SHARE_Q.put(task)

        for i in range(cls._WORKER_THREAD_NUM):
            thread = MyThread(cls.worker)
            time.sleep(0.37)
            # thread.setDaemon(True)
            thread.start()
            thread.join()
            # threads.append(thread)
        # for thread in threads:
        #     thread.join()
        cls.SHARE_Q.join()


###宅配通###	配送完成
class e_can(request, MyThread):

    SHARE_Q = queue.Queue()
    _WORKER_THREAD_NUM = 5

    @classmethod
    def parse_ecan(cls, item):

        # pack_no = str(item)
        pack_no = str(item[1])
        ord_num = item[0]

        url = 'http://query2.e-can.com.tw/self_link/id_link_c.asp?txtMainid={}'.format(pack_no)
        # data = {'txtMainID_1':str(pack_no),
        #         'txtMainID_6':'',
        #         'txtMainID_2':'',
        #         'txtMainID_7':'',
        #         'txtMainID_3':'',
        #         'txtMainID_8':'',
        #         'txtMainID_4':'',
        #         'txtMainID_9':'',
        #         'txtMainID_5':'',
        #         'txtMainID_10':'',
        #         'B1':'(unable to decode value)'}

        attempts = 0
        while attempts < 5:
            try:
                result = request.get_page_utf8(url)

                ecan_list = list()
                arrival = 0

                remove_list = ['宅配單號', '貨物狀態', '說明',
                               '日期 / 時間', '作業站']

                if result.find_all('div', {'align': 'center'}):
                    for i in result.find_all('div', {'align': 'center'}):
                        if i.text not in remove_list:
                            text = i.text.replace('\n', '')
                            ecan_list.append(text)
                            if '配送完成' in text:
                                arrival = 1
                            if text is None:
                                log.WRITE('宅配通', '{}, HTML格式可能改變'.format(pack_no))

                else:
                    update(0, pack_no)
                    break

                body = [{'宅配單號': ecan_list[i],
                         '貨物狀態': ecan_list[i+1],
                         '說明': ecan_list[i+2],
                         '日期/時間': ecan_list[i+3],
                         '作業站': ecan_list[i+4]} for i in range(0, len(ecan_list), 5)]

                now = datetime.datetime.today().strftime("%Y-%m%d-%H:%M:%S")
                doc = {
                    'ORD_NUM': ord_num,
                    'PACKAGE_NO': pack_no,
                    'PACKAGE_STATUS': body,
                    'FLAG': arrival,
                    'UPDATE_TIME': now
                }

                connection.ELK.handle_ES('宅配通', 'ECAN', doc, pack_no)

                if arrival == 1:
                    update(1, pack_no)
                else:
                    update(0, pack_no)

                break

            except Exception as e:
                attempts += 1
                if attempts == 4:
                    log.WRITE('宅配通', '{}, {}'.format(pack_no, e))

    @classmethod
    def worker(cls):
        while True:
            if not cls.SHARE_Q.empty():
                item = cls.SHARE_Q.get()
                cls.parse_ecan(item)
                cls.SHARE_Q.task_done()
            else:
                break

    @classmethod
    def ecan_main(cls):
        # a = [401217051244, 778013884524, 777049625796]

        sql_stat = ('''select [ORD_NUM], [PACKAGE_NO] from [dbo].[LOGISTIC_STATUS]
                       where [SCT_DESC] = '台灣宅配通' and [PACKAGE_STATUS] = 0 ''')
        result = connection.db('AZURE').do_query(sql_stat)

        threads = []

        for task in result:
            cls.SHARE_Q.put(task)

        for i in range(cls._WORKER_THREAD_NUM):
            thread = MyThread(cls.worker)
            time.sleep(0.27)
            # thread.setDaemon(True)
            thread.start()
            # threads.append(thread)
        # for thread in threads:
            thread.join()
        cls.SHARE_Q.join()


###嘉里###配達,已完成簽收
class ktj(request, MyThread):

    SHARE_Q = queue.Queue()
    _WORKER_THREAD_NUM = 5

    @classmethod
    def parse_ktj(cls, item):

        pack_no = str(item[1])
        ord_num = item[0]

        url = 'https://www.kerrytj.com/ZH/search/table_list.aspx'
        data = {'gno':pack_no}
        # url = 'https://www.kerrytj.com/zh/search/search_track_list.aspx'
        # data = {
        #     'rdType': '0',
        #     'trackNo1': str(pack_no),
        #     'trackNo2': '',
        #     'trackNo3': '',
        #     'trackNo4': '',
        #     'trackNo5': '',
        #     'btnTrack': 'Submit'
        # }
        attempts = 0
        while attempts < 5:
            try:
                result = request.get_page_utf8(url, data)

                ktj_list = list()
                arrival = 0

                if result.find_all('td', {'align': 'left'}):
                    for i in result.find_all('td', {'align': 'left'}):
                        ktj_list.append(i.text)
                        if '已完成簽收' in i.text:
                            arrival = 1
                        if i.text is None:
                            log.WRITE('嘉里物流', '{}, HTML格式可能改變'.format(pack_no))
                else:
                    update(0, pack_no)
                    break

                body = [{'日期': ktj_list[i],
                         '時間': ktj_list[i+1],
                         '作業流程': ktj_list[i+2],
                         '所站': ktj_list[i+3]} for i in range(0, len(ktj_list), 4)]

                now = datetime.datetime.today().strftime("%Y-%m%d-%H:%M:%S")
                doc = {
                    'ORD_NUM': ord_num,
                    'PACKAGE_NO': pack_no,
                    'PACKAGE_STATUS': body,
                    'FLAG': arrival,
                    'UPDATE_TIME': now
                }

                connection.ELK.handle_ES('嘉里物流', 'KTJ', doc, pack_no)

                if arrival == 1:
                    update(1, pack_no)
                else:
                    update(0, pack_no)

                break

            except Exception as e:
                attempts += 1
                if attempts == 4:
                    log.WRITE('嘉里物流', '{}, {}'.format(pack_no, e))


    @classmethod
    def worker(cls):
        while True:
            if not cls.SHARE_Q.empty():
                item = cls.SHARE_Q.get()
                cls.parse_ktj(item)
                cls.SHARE_Q.task_done()
            else:
                break

    @classmethod
    def ktj_main(cls):
        # a = [93604162732, 93604162731, 93613886548, 93604162628, 99178723441]

        sql_stat = ('''select [ORD_NUM], [PACKAGE_NO] from [dbo].[LOGISTIC_STATUS]
                       where [SCT_DESC] = '嘉里大榮物流' and [PACKAGE_STATUS] = 0 ''')
        result = connection.db('AZURE').do_query(sql_stat)
        threads = []

        for task in result:
            cls.SHARE_Q.put(task)

        for i in range(cls._WORKER_THREAD_NUM):
            thread = MyThread(cls.worker)
            time.sleep(0.27)
            # thread.setDaemon(True)
            thread.start()
            # threads.append(thread)
        # for thread in threads:
            thread.join()

        cls.SHARE_Q.join()



###通盈通運###已送達
class tong_ying(request, MyThread):

    SHARE_Q = queue.Queue()
    _WORKER_THREAD_NUM = 5

    @classmethod
    def parse_tongying(cls, item):

        pack_no = str(item[1])
        ord_num = item[0]

        # url = 'http://www.tong-ying.com.tw/exploitation/search2.php'
        url = 'http://www.tong-ying.com.tw/exploitation/sw.php?on1='
        # data = {
        #     'on1': str(pack_no)
        # }
        attempts = 0
        while attempts < 5:
            try:
                result = request.get_page_big5(url + pack_no)

                ###發送日期	發送站	貨物條碼	收件人	送達日期	代收款	訂單編號	配送狀態###
                tongying_list = list()
                arrival = 0

                remove_list = ['點貨日期', '作業別', '件數',
                               '才數', '作業站所', '車番', '配送狀態', ' ']

                if result.find_all('div', {'align':'center', 'class': 'style2'} ):
                    for i in result.find_all('div', {'align':'center', 'class': 'style2'} ):
                        if i.text not in remove_list:
                            text = i.text.replace(' ', '')
                            tongying_list.append(text)
                            if '已送達' in text:
                                arrival = 1
                            if text is None:
                                log.WRITE('通盈貨運', '{}, HTML格式可能改變'.format(pack_no))
                else:
                    update(0, pack_no)
                    break

                body = [{'點貨日期': tongying_list[i],
                         '作業別': tongying_list[i + 1],
                         '件數': tongying_list[i + 2],
                         '才數': tongying_list[i + 3],
                         '作業站所': tongying_list[i + 4],
                         '車番': tongying_list[i + 5]} for i in range(0, len(tongying_list), 6)]

                now = datetime.datetime.today().strftime("%Y-%m%d-%H:%M:%S")

                doc = {
                    'ORD_NUM': ord_num,
                    'PACKAGE_NO': pack_no,
                    'PACKAGE_STATUS': body,
                    'FLAG': arrival,
                    'UPDATE_TIME': now
                }

                connection.ELK.handle_ES('通盈貨運', 'TONGYING', doc, pack_no)

                if arrival == 1:
                    update(1, pack_no)
                else:
                    update(0, pack_no)

                break

            except Exception as e:
                attempts += 1
                if attempts == 4:
                    log.WRITE('通盈貨運', '{}, {}'.format(pack_no, e))

    @classmethod
    def worker(cls):
        while True:
            if not cls.SHARE_Q.empty():
                item = cls.SHARE_Q.get()
                cls.parse_tongying(item)
                cls.SHARE_Q.task_done()
            else:
                break

    @classmethod
    def tongying_main(cls):

        # a = [6500172856, 7273830761, 7272331651, 7272331636]
        sql_stat = ('''select [ORD_NUM], [PACKAGE_NO] from [dbo].[LOGISTIC_STATUS]
                       where [SCT_DESC] = '通盈通運' and [PACKAGE_STATUS] = 0 ''')
        result = connection.db('AZURE').do_query(sql_stat)
        threads = []

        for task in result:
            cls.SHARE_Q.put(task)

        for i in range(cls._WORKER_THREAD_NUM):
            thread = MyThread(cls.worker)
            time.sleep(0.27)
            # thread.setDaemon(True)
            thread.start()
            # threads.append(thread)
        # for thread in threads:
            thread.join()

        cls.SHARE_Q.join()

###便利帶###送件完成
class maple(request, MyThread):

    SHARE_Q = queue.Queue()
    _WORKER_THREAD_NUM = 5

    @classmethod
    def parse_maple(cls, item):

        # pack_no = str(item)
        pack_no = str(item[1])
        ord_num = item[0]

        ##查無條碼(6週)
        url = 'http://www.25431010.tw/Search.php'

        payload = {
            'tik': '10156821981494572912',
            'BARCODE1': pack_no,
            'BARCODE2': '',
            'BARCODE3': ''
        }

        headers = {
            'origin': "http://www.25431010.tw",
            'upgrade-insecure-requests': "1",
            'user-agent': "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36",
            'content-type': "application/x-www-form-urlencoded",
            'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            'referer': "http://www.25431010.tw/Search.php",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4",
            'cache-control': "no-cache"
        } #'cookie': "FSESSIONID=cm7tffd0jh70d5ljbp4h54tm26",
        attempts = 0
        while attempts < 5:
            try:
                response = requests.request("POST", url, data=payload, headers=headers)
                result = BeautifulSoup(response.text, 'lxml')

                # tik_html = request.get_page_utf8(url)
                # tik_values = None
                # for i in tik_html.find_all('input', {'name': 'tik'}):
                #     tik_values = i['value']
                # # print(tik_values)
                #
                # data = {
                #     'tik':tik_values,
                #     'BARCODE1':'',
                #     'BARCODE2':str(pack_no),
                #     'BARCODE3':''
                # }
                #
                # result = request.get_page_utf8(url, data)

                maple_list = list()
                arrival = 0
                remove_list = ['配送歷程']
                # 查無條碼

                if result.find_all('td', {'align': 'center', 'bgcolor': '#FFFFCC'}):
                    for i in result.find_all('td', {'align': 'center', 'bgcolor': '#FFFFCC'}):
                        if i.text not in remove_list:
                            text = i.text.replace('\xa0', '')
                            maple_list.append(text)
                            if '送件完成' in text:
                                arrival = 1
                            if text is None:
                                log.WRITE('便利帶', '{}, HTML格式可能改變'.format(pack_no))
                else:
                    update(0, pack_no)
                    break

                body = [{'條碼': maple_list[i],
                         '日期': maple_list[i + 1],
                         '目前狀態': maple_list[i + 2]} for i in range(0, len(maple_list), 3)]

                now = datetime.datetime.today().strftime("%Y-%m%d-%H:%M:%S")

                doc = {
                    'ORD_NUM': ord_num,
                    'PACKAGE_NO': pack_no,
                    'PACKAGE_STATUS': body,
                    'FLAG': arrival,
                    'UPDATE_TIME': now
                }

                connection.ELK.handle_ES('便利帶', 'MAPLE', doc, str(pack_no))

                if arrival == 1:
                    update(1, pack_no)
                else:
                    update(0, pack_no)

                break

            except Exception as e:
                attempts += 1
                if attempts == 4:
                    log.WRITE('便利帶', '{}, {}'.format(pack_no, e))


    @classmethod
    def worker(cls):
        while True:
            if not cls.SHARE_Q.empty():
                item = cls.SHARE_Q.get()
                cls.parse_maple(item)
                cls.SHARE_Q.task_done()
            else:
                break

    @classmethod
    def maple_main(cls):

        # a = [760057691258, 860009368709, 610009957074]
        # a = ['610006104803']
        sql_stat = ('''select [ORD_NUM], [PACKAGE_NO] from [dbo].[LOGISTIC_STATUS]
                       where [SCT_DESC] = '豐業物流(便利帶)' and [PACKAGE_STATUS] = 0 ''')
        result = connection.db('AZURE').do_query(sql_stat)
        threads = []

        for task in result:
            cls.SHARE_Q.put(task)

        for i in range(cls._WORKER_THREAD_NUM):
            thread = MyThread(cls.worker)
            time.sleep(0.27)
            # thread.setDaemon(True)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        cls.SHARE_Q.join()

def update(status, pack_no):

    sql_stat = ('''update [dbo].[LOGISTIC_STATUS]
                   set [PACKAGE_STATUS] = %s, [PARSE_DATE] = CONVERT(VARCHAR(19), GETDATE(), 120)
                   where [PACKAGE_NO] = %s''')
    connection.db('AZURE').do_query(sql_stat, (status, str(pack_no)))


def main():

    SHARE_Q = queue.Queue()
    _WORKER_THREAD_NUM = 4

    def worker():

        while True:
            if not SHARE_Q.empty():
                item = SHARE_Q.get()
                item()
                SHARE_Q.task_done()
            else:
                break

    def start():

        ### put method into list
        a = [t_cat.tcat_main, hct.hct_main, pstmail.pstmail_main, ktj.ktj_main,
             maple.maple_main, tong_ying.tongying_main, e_can.ecan_main]
        # a = [hct.hct_main, pstmail.pstmail_main]
        threads = []

        for task in a:
            SHARE_Q.put(task)

        for i in range(_WORKER_THREAD_NUM):
            thread = MyThread(worker)
            time.sleep(0.27)
            # thread.setDaemon(True)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        SHARE_Q.join()

    start()

if __name__ == '__main__':

    main()
    print('Finish')
    # pstmail.pstmail_main()
    # sys.exit()

