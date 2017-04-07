# ！coding=utf8
from matplotlib import dates
import requests
import datetime
import cPickle
import os
import re
import json
import MySQLdb
from stockAnalysis2 import tecindex

'''
利用网易的api，下载股票历史数据
上证指数
http://quotes.money.163.com/service/chddata.html?code=0000001&start=19901219&end=20150731&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER
八一钢铁
http://quotes.money.163.com/service/chddata.html?code=0600581&start=20020813&end=20150731&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP
天虹商场
http://quotes.money.163.com/service/chddata.html?code=1002419&start=20100519&end=20150731&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP
'''


def getquotes(code='0000001'):
    now = datetime.datetime.now()
    filename = code + '_' + now.strftime('%Y%m%d')
    datadir = 'data'
    if not os.path.isfile(datadir + os.path.sep + filename):
        preurl = 'http://quotes.money.163.com/service/chddata.html?code='
        start = '&start='
        end = '&end='
        posturl1 = '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'
        posturl2 = '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
        html = None
        if code == '0000001':
            html = requests.get(preurl + code + start + '19901219' + end + now.strftime('%Y%m%d') + posturl1)
        else:
            html = requests.get(preurl + code + start + '19901219' + end + now.strftime('%Y%m%d') + posturl2)
        result = []
        io = 0
        ic = 0
        ih = 0
        il = 0
        iv = 0
        for i, line in enumerate(html.content.split('\n')):
            if i == 0:
                datas = line.split(',')
                # print datas
                io = datas.index(u'开盘价'.encode('gbk'))
                ic = datas.index(u'收盘价'.encode('gbk'))
                ih = datas.index(u'最高价'.encode('gbk'))
                il = datas.index(u'最低价'.encode('gbk'))
                iv = datas.index(u'成交量'.encode('gbk'))
            if i > 0 and line != '':
                line = line.strip()
                line = line.split(',')
                d = datetime.datetime.strptime(line[0], '%Y-%m-%d')
                dtime = dates.date2num(d)
                o = float(line[io])
                h = float(line[ih])
                l = float(line[il])
                c = float(line[ic])
                v = int(line[iv])
                if o != 0 and h != 0 and l != 0 and c != 0:
                    result.append((dtime, o, h, l, c, v))

        result.reverse()
        tmp = open(datadir + '/' + filename, 'wb')
        cPickle.dump(result, tmp)
        tmp.close()
        return result
    else:
        tmp = open(datadir + os.path.sep + filename, 'rb')
        result = cPickle.load(tmp)
        tmp.close()
        return result


def updatedata_sina(code='sh000001'):
    api = 'http://hq.sinajs.cn/list='
    result = requests.get(api + code)
    data = re.compile('"(.*?)"')
    try:
        info = data.findall(result.content)[0]
        info = info.split(',')
        name = info[0].decode('gbk')
        o = info[1]
        c = info[3]
        h = info[4]
        l = info[5]
        v = info[8]
        return code[2:], name, float(o), float(h), float(l), float(c), int(v)
    except Exception as e:
        print info


def updatedata_163(code='0000001'):
    api = 'http://api.money.126.net/data/feed/'
    result = requests.get(api + code)
    data = re.compile('_ntes_quote_callback\((.*?)\);')
    try:
        info = data.findall(result.content)[0]
        info = json.loads(info)
        name = info[code]['name']
        o = info[code]['open']
        c = info[code]['price']
        h = info[code]['high']
        l = info[code]['low']
        v = info[code]['volume'] / 100
        return code[1:], name, float(o), float(h), float(l), float(c), int(v)
    except Exception as e:
        print info


def intomysqlcode():
    conn = MySQLdb.connect(host='localhost', user='root', passwd='', db='quotes', charset='utf8')
    cursor = conn.cursor()
    for i, line in enumerate(open('stocklist.csv')):
        if i > 0:
            line = line.strip()
            code, name = line.split(',')
            code, shsz = code.split('.')
            name = name.decode('gbk')
            try:
                cursor.execute(u'insert into code(code,name,type) values("{0}","{1}","{2}")'.format(code, name, shsz))
            except Exception as e:
                print e
                conn.rollback()

                # quotes = cPickle.load(open('data/'+code+'_20150805'))
                # print len(quotes)
    conn.commit()
    conn.close()


def intomysqlquote():
    '''
    跟新所有code表中的股票
    从163 api 中获取历史数据
    :return:
    '''
    conn = MySQLdb.connect(host='localhost', user='root', passwd='', db='quotes', charset='utf8')
    cursor = conn.cursor()
    cursor.execute(u'select name,code,type from code')
    for line in cursor.fetchall():
        name, code, t = line
        if t == 'SH':
            code = '0' + code
        else:
            code = '1' + code
        try:
            datas = getquotes(code=code)
            print u'processing %s  -------%s  is processed' % (code, name)
            for d in datas:
                cursor.execute(
                    u'insert into quotes(code,name,date,open,high,low,close,volume) values("{0}","{1}","{2}","{3}",{4},{5},{6},{7})'.format( \
                        code[1:], name, dates.num2date(d[0]).strftime("%Y-%m-%d"), d[1], d[2], d[3], d[4], d[5] / 100))
            conn.commit()
        except Exception as e:
            print e
            conn.rollback()
    conn.close()


def updateall():
    '''
    根据 code 表中的数据
    从163 api 实时跟新数据
    :return:
    '''
    conn = MySQLdb.connect(host='localhost', user='root', passwd='', db='quotes', charset='utf8')
    cursor = conn.cursor()
    cursor.execute(u"select * from code")
    result = cursor.fetchall()
    for name, code, t in result:
        try:
            tmp_code = None
            if t == 'SH':
                tmp_code = '0' + code
            else:
                tmp_code = '1' + code
            print "updating %s" % tmp_code
            d = updatedata_163(tmp_code)
            name = name
            # 写入前需要加检查数据库是否存在,！！！！在predict表中，加入preclose
            #也需要检查是否为0
            if d[2]!=0 and d[3]!=0 and d[4]!=0 and d[5]!=0 and d[6]!=0:
                cursor.execute(u'select * from quotes where name="{0}" and code="{1}" and date="{2}"'.format(name,code,datetime.datetime.now().strftime("%Y-%m-%d")))
                tmp = cursor.fetchall()

                if len(tmp)<1:
                    #跟新predict表的truth,计算这次收盘价与最近一次的收盘价的差，<=0 --> 0; >0 -->1

                    cursor.execute(u'select date,close from quotes where name="{0}" and code = "{1}" order by date DESC limit 1 '.format(
                        name,code
                    ))
                    pclose = cursor.fetchone()
                    cursor.execute(u'select * from  predict  where code ="{1}" and name="{2}" and date="{3}"'.format(
                        code,name,pclose[0].strftime("%Y-%m-%d")
                    ))
                    if len(cursor.fetchall())>0:
                        truth = 0
                        if d[5]-pclose[1]>0:
                            truth = 1
                        cursor.execute(u'update predict set truth = {0} where code ="{1}" and name="{2}" and date="{3}"'.format(
                            truth,code,name,pclose[0].strftime("%Y-%m-%d")
                        ))
                    cursor.execute(
                        u'insert into quotes(code,name,date,open,high,low,close,volume) values("{0}","{1}","{2}","{3}",{4},{5},{6},{7})'.format( \
                            code, name, datetime.datetime.now().strftime("%Y-%m-%d"), d[2], d[3], d[4], d[5], d[6]))
                    conn.commit()

        except Exception as e:
            print e
            conn.rollback()

    conn.close()


if __name__ == "__main__":
    updateall()
