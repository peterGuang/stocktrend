#!coding=utf8

import datetime
import zipfile
import requests
import os
from bs4 import BeautifulSoup
import re
import math
import time
import random

"""
一个单线程抓取sina新闻列表的程序
一共有三个时间段的不同列表：
list1 （1999，5，26）-（2007，1，19）
list2 （2007，1，20）-（2010，3，29）
list3 （2010，3，30）- now
++++++++++++++++++++++++++
list1 抓取规则
step1：根据规则生成url，下载页面
step2：parse页面，获取四元组<category,title,url,time>
++++++++++++++++++++++++++
list2 抓取规则
step1：根据规则生成 9个不同category的url
step2：直接根据url获取数据（json格式），解析json格式数据，
        获取<category,title,url,time>四元组
++++++++++++++++++++++++++
list3 抓取规则
step1：根据规则生成url
step2：跟url获取数据（json格式），解析json数据，
       获得总条目数，获得翻页数，逐页获取数据
       <category,title,url,time>四元组
++++++++++++++++++++++++++
数据储存规格是txt文件，全部存到一个文件，每1w个四元组存一个zip文件。
++++++++++++++++++++++++++
主要模块：
1，url生成模块：初始化为（1999，5，26），
   函数next_day():生成url列表，调用parser模块，返回四元组
2，下载模块：输入url，返回内容，错误处理（超时，404，200等）
3，parser模块：输入内容，以及页面类型，返回四元组list
4，存储管理：put（四元组list），缓存10000个四元组，达到100W个，
   写zip文件，清空缓存，从头开始储存。
5，四元组:{"cagegory":c,"title":t,"url":u,"dtime":t}
6，定义页面类型｛
    'HTML':0,   #一个html页面
    'JSON':1,   #九个不同的json url
    'JSONPAGE':2#分页的json url
    ｝
"""


class Utils():
    def __init__(self):
        raise NotImplementedError

    name2num = {
        'HTML': 0,
        'JSONPAGE1': 1,
        'JSONPAGE2': 2,
        'JSONPAGE3': 3 }
    num2name = {
        0: 'HTML',
        1: 'JSONPAGE1',
        2: 'JSONPAGE2',
        3: 'JSONPAGE3'
    }
    DATA_DIR = 'url_lists'
    URL_CACHE = []
    FILE_COUNT = 24
    MAX = 1e6
    d1 = datetime.datetime(1999, 5, 26)
    d2 = datetime.datetime(2007, 1, 20)
    d3 = datetime.datetime(2007, 12, 12)
    d4 = datetime.datetime(2010, 3, 30)
    url_1 = 'http://news.sina.com.cn/old1000/news1000_'
    url_2 = {'国内': 'http://rss.sina.com.cn/rollnews/news_gn/', '国际': 'http://rss.sina.com.cn/rollnews/news_gj/',
             '社会': 'http://rss.sina.com.cn/rollnews/news_sh/', '军事': 'http://rss.sina.com.cn/rollnews/jczs/',
             '体育': 'http://rss.sina.com.cn/rollnews/sports/', '娱乐': 'http://rss.sina.com.cn/rollnews/ent/',
             '科技': 'http://rss.sina.com.cn/rollnews/tech/', '金融': 'http://rss.sina.com.cn/rollnews/finance/',
             '股票': 'http://rss.sina.com.cn/rollnews/stock/'}
    url_3_pre = 'http://roll.news.sina.com.cn/interface/rollnews_ch_out_interface.php?col=89&spec=&type=&date='
    url_3_post = '&ch=01&k=&offset_page=0&offset_num=0&num=60&asc=&page='


def page_type_name(num):
    """
    根据页面类型编号，返回页面类型名称
    :param num:
    :return:
    """
    return Utils.num2name[num]


def page_type_num(name):
    """
    根据页面类型名称，返回页面类型编号
    :param name:
    :return:
    """
    return Utils.name2num[name]


class SinaURL():
    """
    5，四元组模块：实现__str__
    """

    def __init__(self, category=None, title=None, url=None, dtime=None):
        """
        :param category:
        :param title:
        :param url:
        :param dtime:
        """
        self.category = category
        self.title = title
        self.url = url
        self.dtime = dtime



class DataCache():
    """
    4，存储管理：put（四元组list），缓存10000个四元组，达到100W个，
    写zip文件，清空缓存，从头开始储存。
    """

    def __init__(self):
        pass

    def put(self, urllist):
        for sinaurl in urllist:
            Utils.URL_CACHE.append(sinaurl)
            if len(Utils.URL_CACHE) == Utils.MAX:
                self.save()

    def save(self):
            print "start Saving",Utils.FILE_COUNT,"batch"
            zipout = zipfile.ZipFile("{0}{1}{2}{3}".format(Utils.DATA_DIR, '/', str(Utils.FILE_COUNT), '.zip'), 'w', zipfile.ZIP_DEFLATED)
            print "wring file..."
            filename = "{0}{1}{2}".format(Utils.DATA_DIR, '/', str(Utils.FILE_COUNT))
            out = open(filename, 'w')
            for i in range(len(Utils.URL_CACHE)):
                line = Utils.URL_CACHE.pop()
                category = line.category
                title = line.title
                url = line.url
                dtime = line.dtime

                try:
                    out.write(dtime.encode('utf8')+'\t')
                    out.write(url.encode('utf8')+'\t')
                    out.write(category.encode('utf8')+'\t')
                    out.write(title.encode('utf8')+'\t')
                except Exception as e:
                    out.write(category.decode('utf8').encode('utf8')+'\t')
                    out.write(title.decode('utf8').encode('utf8')+'\t')
                finally:
                    out.write('\n')
            out.close()
            print "ziping file..."
            zipout.write(filename=filename)
            zipout.close()
            print "removing raw file"
            os.remove(filename)
            Utils.FILE_COUNT += 1


def page_type(date):
    """
    输入日期，返回page的类型
    :rtype : object
    """
    if Utils.d1 <= date < Utils.d2:
        return page_type_name(0)
    elif Utils.d2 <= date < Utils.d3:
        return page_type_name(1)
    elif Utils.d3 <= date < Utils.d4:
        return page_type_name(2)
    elif date >= Utils.d4:
        return page_type_name(3)
    else:
        raise Exception("Date error, no news data!!")


class SinaNewsCrawler():
    def __init__(self,year = 1999,month = 5,day = 25):
        self.date = datetime.datetime(year,month,day)
        #self.date = datetime.datetime(1995, 5, 25)
        #self.date = datetime.datetime(2007, 1, 20)
        #self.date = datetime.datetime(2007, 12, 12)
        #self.date = datetime.datetime(2010, 3, 30)
        self.delta = datetime.timedelta(1)

    def get_page(self, url):
        """
        2，下载模块：输入url，返回内容，错误处理（超时，404，200等）
        """
        timeout = 5
        try:
            response = requests.get(url,timeout=timeout)
            if response.status_code==200:
                return response.content
            elif response.status_code==503:
                print "网络超时"
                return -1
            else:
                return 0
        except Exception as e:
            print e
            return 0


    def parser(self, pagetype, html,debug=True):
        """
        3，parser模块：输入内容，以及页面类型，返回四元组list
        """
        if html==-1 or html==0:
            return -1
        if pagetype == page_type_name(0):
            sinalist = []
            pattern_news = re.compile('<li>.*?<br>')
            pattern_title = re.compile('target=_blank>(.*?)<?/')
            pattern_category = re.compile('>\[(.*?)\] *?[</\.]')
            pattern_url = re.compile("a href=['\"]?(.*?)['\"]? ")
            pattern_dtime = re.compile('> \((.*?)\)<')
            html=html.decode('gbk','ignore')
            html=html.replace('\n','')
            html=html.lower()
            if self.date>=datetime.datetime(2000,1,1) and self.date<=datetime.datetime(2005,3,1):
                pattern_news=re.compile('<li>.*?</font>')
            elif self.date>=datetime.datetime(2005,3,2):
                pattern_news = re.compile('<li>.*?\d{2}:\d{2}')
                pattern_category = re.compile('\[(.*?)\]')
                pattern_title =re.compile('<a href.*?>(.*?)</a>')
                pattern_url = re.compile("href=['\"]?(.*?)['\"]?>")
                pattern_dtime = re.compile("</a>(.*)")
            newslist = pattern_news.findall(html)
            if len(newslist)>0:

                for news in newslist:
                    try:
                        sa = SinaURL()
                        sa.title= pattern_title.findall(news)[0]
                        sa.category = pattern_category.findall(news)[0]
                        sa.dtime =pattern_dtime.findall(news)[0]
                        sa.dtime = sa.dtime.replace('&nbsp;',' ')
                        sa.dtime = self.date.strftime('%Y-%m-%d ')+sa.dtime
                        sa.url = pattern_url.findall(news)[0]
                        sa.url = sa.url.replace('..','http://news.sina.com.cn')
                        sinalist.append(sa)
                    except Exception as e:
                        if debug:
                             print news
                             print sa.title
                             print sa.category
                             print sa.url
                             print sa.dtime
                        print e
                return sinalist

            else:
                #print html
                pattern_news = re.compile('<img src="/images/ball.gif".*?<br>')
                pattern_title = re.compile('target=_blank>(.*?)<')
                pattern_category = re.compile('>\[(.*?)\]')
                pattern_url = re.compile("href=['\"' ](.*?)['\"' ]")
                pattern_dtime = re.compile('> \((.*?)\)<')
                #html=html.decode('gbk','ignore')
                newslist = pattern_news.findall(html)

                for news in newslist:
                    try:
                        sa = SinaURL()
                        sa.title= pattern_title.findall(news)[0]
                        sa.category = pattern_category.findall(news)[0]
                        sa.dtime =pattern_dtime.findall(news)[0]
                        sa.dtime = sa.dtime.replace('&nbsp;',' ')
                        sa.url = pattern_url.findall(news)[0]
                        sinalist.append(sa)
                    except Exception as e:
                        if debug:
                            print news
                            print sa.title
                            print sa.category
                            print sa.url
                            print sa.dtime
                        print e
                return sinalist
        elif pagetype == page_type_name(1):
            urllist = re.search('var sinaRss =(.*);',html.decode('gbk','ignore')).group(1)
            urllist =  eval(urllist)
            sinalist = []
            for url in urllist:
                try:
                    sa = SinaURL()
                    sa.category = url[0]
                    sa.title = url[2]
                    sa.url = url[3]
                    sa.dtime= url[4]
                    sinalist.append(sa)
                    #print sa.dtime
                except Exception as e:
                    print e
            return sinalist
        elif pagetype == page_type_name(2):
            html = html.decode('gbk','ignore')
            html=html.replace('\r','')
            html=html.replace('\n','')
            html=html.replace('\t','')
            html=html.replace('category','"category"')
            html=html.replace('cLink','"cLink"')
            html=html.replace('subcol','"subcol"')
            html=html.replace('title','"title"')
            html=html.replace('link','"link"')
            html=html.replace('pubDate','"pubDate"')
            html=html.replace('item','"item"')
            html=html.replace('&#8226;','')
            #print html[15:-92]
            urllist = html[14:-92]
            urllist = eval(urllist)
            sinalist = []
            for url in urllist['item']:
                sa = SinaURL()
                sa.category = url['category']
                sa.title = url['title']
                sa.url = url['link']
                sa.dtime= url['pubDate']
                #print sa.dtime
                sinalist.append(sa)
            return sinalist
        elif pagetype == page_type_name(3):
            html = html.decode('gbk','ignore')
            urllist = html[14:-1]
            urllist=urllist.replace('serverSeconds :','"serverSeconds":')
            urllist=urllist.replace('last_time :','"last_time":')
            urllist=urllist.replace('path :','"path":')
            urllist=urllist.replace('title :','"title":')
            urllist=urllist.replace('id :','"id":')
            urllist=urllist.replace('cType :','"cType":')
            urllist=urllist.replace('count :','"count":')
            urllist=urllist.replace('offset_page :','"offset_page":')
            urllist=urllist.replace('offset_num :','"offset_num":')
            urllist=urllist.replace('list :','"list":')
            urllist=urllist.replace('channel :','"channel":')
            urllist=urllist.replace('url :','"url":')
            urllist=urllist.replace('type :','"type":')
            urllist=urllist.replace('pic :','"pic":')
            urllist=urllist.replace('time :','"time":')
            urllist=urllist.replace('last_"time":','last_time')
            urllist = eval(urllist)
            sinalist = []
            for url in urllist['list']:
                #print url
                sa = SinaURL()
                sa.category = url['channel']['title']
                sa.title = url['title']
                sa.url = url['url']
                sa.dtime= datetime.datetime.fromtimestamp(int(url['time'])).strftime("%Y/%m/%d %H:%M")
                sinalist.append(sa)
                #print sa.dtime
            return sinalist, int(urllist['count'])
        else:
            return -1

    def next_day(self,debug=True):
        """
        1，url生成模块：初始化为（1999，5，26），
        函数next_day():生成url列表，调用parser模块，返回四元组
        """
        self.date += self.delta
        pagetype = page_type(self.date)
        if pagetype == page_type_name(0):
            try:
                params = self.date.strftime("%Y%m%d")
                postfix = '.shtml'
                url = Utils.url_1 + params + postfix
                print url
                html = self.get_page(url)
                return self.parser(pagetype, html,debug)
            except Exception as e:
                print e
        elif pagetype==page_type_name(1):
            params = self.date.strftime("%Y%m%d")+'/data'
            urllist = []
            pageno = str(0)
            postfix = '.js'
            url = Utils.url_1 + params + pageno + postfix
            print url
            html = self.get_page(url)
            suburllist = self.parser(pagetype,html,debug)
            total_num = re.search("var totalNews = (.*?);",html).group(1)
            urllist.extend(suburllist)
            firstpage= 100.0
            pagesize = 500.0
            for i in range(1,int(math.ceil((int(total_num)-100+1.0)/pagesize)+1)):
                try:
                    url = Utils.url_1 + params + str(i) + postfix
                    print url
                    html = self.get_page(url)
                    suburllist = self.parser(pagetype,html,debug)
                    urllist.extend(suburllist)
                except Exception as e:
                    print e
            return urllist

        elif pagetype == page_type_name(2):
            params = self.date.strftime("%Y%m%d")
            postfix = '.js'
            urllist = []
            for category_name in Utils.url_2.keys():
                try:
                    url = Utils.url_2[category_name] + params + postfix
                    print url
                    html = self.get_page(url)
                    suburllist = self.parser(pagetype,html,debug)
                    #print len(suburllist),category_name
                    urllist.extend(suburllist)
                except Exception as e:
                    print e
            return urllist
        elif pagetype == page_type_name(3):
            params = self.date.strftime("%Y-%m-%d")
            pageno = str(1)
            urllist = []
            url = "{0}{1}{2}{3}".format(Utils.url_3_pre, params, Utils.url_3_post, pageno)
            html = self.get_page(url)
            print url
            suburllist ,total_num= self.parser(pagetype,html,debug)
            urllist.extend(suburllist)
            for i in range(2,int(math.ceil(total_num*1.0/60))+1):
                try:
                    url = "{0}{1}{2}{3}".format(Utils.url_3_pre, params, Utils.url_3_post, str(i))
                    html = self.get_page(url)
                    suburllist ,num= self.parser(pagetype, html,debug)
                    urllist.extend(suburllist)
                except Exception as e:
                    print e
            return urllist


if __name__ == "__main__":
    s = SinaNewsCrawler(2014,6,6)
    #s = SinaNewsCrawler(2000,12,13)
    #html = s.get_page('http://news.sina.com.cn/old1000/news1000_19990607.shtml')
    #s.parser(page_type_name(0),html)
    d = DataCache()
    print (datetime.datetime.now()-datetime.datetime(1999, 5, 25)).days
    for i in range((datetime.datetime.now()-datetime.datetime(1999, 5, 25)).days):
        try:
            urllist = s.next_day(debug=True)
            print len(urllist)
            d.put(urllist)

            #break
            #time.sleep(random.randint(1,1000)*1.0/1000)
        except Exception as e:
            print e
    d.save()