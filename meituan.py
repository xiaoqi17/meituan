# -*- coding: utf-8 -*-
import re

import pymongo
import requests
import time
from bs4 import BeautifulSoup
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

client = pymongo.MongoClient('localhost', 27017)
ceshi = client['meituan']
item_infoA = ceshi['shop_details']
item_infoB = ceshi['comment']

'''采集商品链接'''
def index_html(url,headers):
    response = requests.get(url,headers=headers)
    response.encoding=response.apparent_encoding
    if response.status_code == 200:
        text=response.text
        soup=BeautifulSoup(text,'lxml')
        links = soup.select('#deals > dl.list > dd.poi-list-item > a')
        for link in links:
            url = link.get('href')
            yield url
        '''这里通过构建翻页功能并回掉给index_html，进行下一页'''
        next_pages = soup.select('#deals > dl:nth-of-type(16) > dd > div > a:nth-of-type(2)')
        for next_page in next_pages:
            next_page = 'http://i.meituan.com/'+str(next_page)+'&cid=1'
            index_html(next_page,headers)

'''采集商店详情'''
def page_html(url,headers):
    if item_infoA.find_one({'商店链接': url}):
        print '%s爬过' % url
    else:
        '''移动端在跳转到商家详情页时，会出现提示下载APP还是继续触屏版，选择触屏版'''
        s = requests.session() #保持cookie
        response = s.get(url, headers=headers)
        time.sleep(1)
        response.encoding = response.apparent_encoding
        if response.status_code == 200:
            text = response.text
            soup = BeautifulSoup(text, 'lxml')
            links = soup.select('body > div.info > div.go-visit > a')
            for link in links:
                urls = link.get('href')
                '''点击继续触屏版真正来的商家详情页面'''
                response = s.get(urls, headers=headers)
                time.sleep(5)
                response.encoding = response.apparent_encoding
                if response.status_code == 200:
                    text = response.text
                    soup = BeautifulSoup(text, 'lxml')
                    titles = soup.select('div.name > p')
                    stras = soup.select('span.score > span > em')
                    addrs = soup.select('div.poi-address ')
                    comment_urls = soup.select('div.buy-comments > a')
                    prices = soup.select(' span.avg-price')
                    for title,stra,addrs,comment_url,price in zip(titles,stras,addrs,comment_urls,prices):
                        data = {
                            '商店':title.get_text().strip(),
                            '评分':stra.get_text().strip(),
                            '地址':addrs.get_text().strip(),
                            '人均消费':price.get_text().strip('人均：¥'),
                            '商店链接':url
                        }
                        print data
                        item_infoA.insert_one(data)
                        link = comment_url.get('href')
                        yield link

'''采集前商店100页的评论'''
def comment_html(url,headers):
    for i in range(1,101):
        link = url + '/page_{}'.format(i)
        if item_infoA.find_one({'评论链接': link}):
            print '%s爬过' % link
        else:
            response = requests.get(link, headers=headers)
            text = response.text
            soup = BeautifulSoup(text, 'lxml')
            usenames = soup.select('dd > div > div.user-wrapper > div.user-info-text > div.userInfo > weak')
            stars_times = soup.select('dd > div > div.user-wrapper > div.user-info-text > div.score > weak')
            comments = soup.select('dd > div > div.comment > p')
            for usename, stars_time, comment in zip(usenames, stars_times, comments):
                data = {
                    '用户名': usename.get_text().strip(),
                    '评价时间': stars_time.get_text().strip(),
                    '评论': comment.get_text().strip(),
                    '评论链接': link
                }
                print data
                item_infoB.insert_one(data)




def main():
    url = 'http://i.meituan.com/guangzhou?cid=1'
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36',

    }
    urls = index_html(url,headers)
    for url in urls:
        links = page_html(url, headers)
        for link in links:
            comment_html(link, headers)

if __name__ == '__main__':
    main()