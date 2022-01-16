# Module import

from bs4 import BeautifulSoup # For BeautifulSoup
import pandas as pd # For DB
from selenium import webdriver # For Selenium
import re # For data cleaning 
import requests

# 접속할 url
# {'에스디바이오센서': 137310} #'https://finance.naver.com/item/news_news.naver?code={Code_num}'
# 뒤에 Code_num 부분을 수정하면 다른 기업의 뉴스 목록 가져올 수 있음

class News : 
    
    # 초기화 함수 
    def __init__(self): 
        self.driver = webdriver.Safari() # safari driver 생성
        self.url_pre = 'https://finance.naver.com'

    # 소멸자 함수
    def __del__(self):
        self.driver.close()

    # page별로 url 가져오기 
    def news_content_crawling(self, code):
        self.url = f'https://finance.naver.com/item/news_news.naver?code={code}' #url 설정
        self.driver.get(self.url) # 해당 페이지 loading

        news_crawling = pd.DataFrame(columns={'title', 'date', 'contents'})
        
        page_html = self.driver.page_source

        # BeautifulSoup로 page Html 파싱
        soup = BeautifulSoup(page_html, 'html.parser')

        ############################
        ### 쪽 별로 new url 가져오기 ###
        ############################

        # html 속 title
        body = soup.select_one('tbody')
        # title이 들어간 a태그 리스트 추출
        news_list = body.select('tr > td.title > a')
        self.news_link_list = {}

       
        for news in news_list:
            title, href = news.string, news.attrs['href'] # 제목, url
            self.news_link_list[title] = href


        for title, link in self.news_link_list.items():

            # new url로 접속해서 페이지 정보 파싱하기 
            news_url = self.url_pre + link  # news 별 url
            self.driver.get(news_url)
            page_html = self.driver.page_source

            soup = BeautifulSoup(page_html, 'html.parser')

            # 본문 내용 가져오기 
            body = soup.select_one('table.view')
            contents = body.select_one('tr > td > div#news_read') 

            # 본문 내용에서 text만 뽑아오기 
            contents_text = contents.get_text()
            
            # 본문 내용 cleaning
            contents_text_re = re.sub('[-=+,#/\?:^$.@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》]',' ', contents_text)
            contents_text_re = ' '.join(contents_text_re.split())
            
            # 날짜 가져오기
            date = body.find("span", class_="p11").get_text()
            
            # DataDrame에 추가하기 
            news = {'title' : title, 'date': date, 'contents': contents_text_re}
            news_crawling = news_crawling.append(news, ignore_index=True) 

        news_crawling.to_csv('news_crawling.csv', index=None, header=True)

    def price_crawling(self, code):
        self.url = f'https://finance.naver.com/item/sise_day.naver?code={code}&page=1' #url 설정
        self.driver.get(self.url) # 해당 페이지 loading

        price_crawling = pd.DataFrame()
        
        ############################
        ### 쪽 별로 new url 가져오기 ###
        ############################

        page_bar = self.driver.find_element_by_class_name('Nnavi')
        pages = page_bar.find_elements_by_css_selector('a')

        ################### 수정 필요 ######################
        page_now = '1'
        for p in pages:
            page_num = p.text.strip()
            page_html = self.driver.page_source
            print(p.text)
            print(self.driver.current_url)
            if page_num in ['맨앞', '이전', '맨뒤']:
                pass
            elif page_num == '다음':
                p.send_keys('\n')
            elif int(page_num) > int(page_now):
                p.send_keys('\n')
                page_now = page_num
            else:
                print('마지막?')
        #################################################   

        # 
        page_html = self.driver.page_source

        # html 속 날짜 + 시세 
        price_crawling = price_crawling.append(pd.read_html(page_html)[0])
        # 빈 값 제거 
        price_crawling.dropna(inplace=True)



if __name__ == "__main__":
    news = News()

    # 뉴스 크롤링
    # news.news_content_crawling(137310)
    
    # 삼성전자 005930 
    # 시세 크롤링 
    news.price_crawling('137310')
