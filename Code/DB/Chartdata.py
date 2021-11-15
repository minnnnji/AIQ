import pymysql, json, calendar, requests, urllib3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from threading import Timer 

urllib3.disable_warnings()

"""
DB명 : Quant
시가총액 코스피/닥 100위 기업 
최근 5년간의 차트데이터 
종목코드(code), 날짜(date), 시가(open), 고가(high), 저가(low),  종가(close), 전일비(diff), 거래량(volume)
"""

class Chartdata:
    def __init__(self):                             #초기화함수 
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host="localhost", user='root', password = "1234", db = "QUANT",charset="utf8")

        ############ 테이블 생성 ############
        with self.conn.cursor() as curs:           #cursor 연결 (DB 연결 커서)
            sql = """
            CREATE TABLE IF NOT EXISTS company_info(        
                code VARCHAR(20),
                company VARCHAR(20),
                last_update DATE,
                PRIMARY KEY (code))
            """                                     #회사정보 테이블 생성 sql문
            curs.execute(sql)

            sql = """
            CREATE TABLE IF NOT EXISTS daily_price (
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (code, date))
            """                                     #일별 가격 테이블 생성 sql문 
            curs.execute(sql)                       #sql문 실행 
            self.conn.commit()                      #DB 최종 저장 
        ###################################

        self.codes = dict()                         #종목코드를 담을 딕셔너리 생성 
        self.update_comp_info()                     #회사정보 업데이트 함수 실행 
    
    def __del__(self):                              #소멸자함수
        """소멸자: MariaDB 연결 해제 """
        self.conn.close()

    def read_comp_info(self):                       #네이버 금융에서 회사 정보 로드                                               
        """네이버 금융에서 코스피/코스닥 시총 100위 기업명, 종목코드 데이터프레임 반환"""
        CompanyInfos = pd.DataFrame()                                                                                               #반환할 데이터프레임 생성  

        for page in range (1,3):                                                                                                    #1 ~ 2 page : page 당, 50개 기업  -> 100위 기업 크롤링 
            try:
                url_kospi = "https://finance.naver.com/sise/sise_market_sum.nhn?sosok=0&page={}".format(page)                             #네이버 금융 코스피 시총 순 페이지 url
                html_kospi = BeautifulSoup(requests.get(url_kospi, verify=False, headers = {'User-agent' : 'Mozilla/5.0'}).text,"lxml")   #요청한 url 가져옴, parser --> lxml (html을 해석하는 패키지)
                titles_kospi = html_kospi.findAll("a", class_="tltle")                                                                    #a 태그 라인 중 class 명이 tltle인 라인을 전체 반환  
                
                if titles_kospi is None:                                                                                                                                                                                              
                    print("title is None")                                                                                           
                    return None   
                
                for idx, title in enumerate(titles_kospi,50*(page-1)+1):
                    company = title.get_text()                                                                                       #회사명 추출                                                       
                    s = str(title["href"]).split('=')                                                                                #하위 태그 a의 href를 텍스트 반환 -> '='기준으로 split 
                    codenum = s[-1]                                                                                                  #종목코드 추출 
                    
                    new_data = {'code':codenum, 'company':company}                                                                   #추출한 회사명, 종목코드 데이터프레임 형식으로 변환 
                    CompanyInfos = CompanyInfos.append(new_data,ignore_index=True)                                                   #회사명, 종목코드 데이터프레임에 저장 
                                                                                                                                     
                    print(f" #{idx} CompanyInfo UPDATE : {codenum} , {company}")                                                     #업데이트 현황 출력   
                   
                url_kosdaq = "https://finance.naver.com/sise/sise_market_sum.nhn?sosok=1&page={}".format(page)                              #네이버 금융 코스닥 시총 순 페이지 url
                html_kosdaq = BeautifulSoup(requests.get(url_kosdaq, verify=False, headers = {'User-agent' : 'Mozilla/5.0'}).text,"lxml")   #요청한 url 가져옴, parser --> lxml (html을 해석하는 패키지)
                titles_kosdaq = html_kosdaq.findAll("a", class_="tltle")                                                                    #a 태그 라인 중 class 명이 tltle인 라인을 전체 반환

                if titles_kosdaq is None:                                                                                                                                                                                              
                    print("title is None")                                                                                           
                    return None   
                
                for idx, title in enumerate(titles_kosdaq,50*(page-1)+1):
                    company = title.get_text()                                                                                       #회사명 추출                                                       
                    s = str(title["href"]).split('=')                                                                                #하위 태그 a의 href를 텍스트 반환 -> '='기준으로 split 
                    codenum = s[-1]                                                                                                  #종목코드 추출 
                    
                    new_data = {'code':codenum, 'company':company}                                                                   #추출한 회사명, 종목코드 데이터프레임 형식으로 변환 
                    CompanyInfos = CompanyInfos.append(new_data,ignore_index=True)                                                   #회사명, 종목코드 데이터프레임에 저장 
                                                                                                                                     
                    print(f" #{idx} CompanyInfo UPDATE : {codenum} , {company}")                                                     #업데이트 현황 출력 


            except Exception as e:                                                                                                   #예외 발생 시, 
                print('Exception occured: ', str(e))                                                                                 #출력     
                return None

        return CompanyInfos                                                                                                          #companyinfo 데이터프레임 반환 
                                                                                                                                        
    def update_comp_info(self):                     #회사정보 DB에 update
        """종목코드와 기업명을 DB에 업데이트 """              
        sql = "SELECT * FROM company_info"                                          #company_info를 선택한다는 sql문 
        df = pd.read_sql(sql, self.conn)                                            #테이블을 읽음
        
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]          #종목코드 딕셔너리에 종목코드와 회사 연결해서 저장 
        
        with self.conn.cursor() as curs:                                            #curs로 cursor를 불러옴
            sql = "SELECT max(last_update) FROM company_info"                       #company_info에서 last_update값을 불러옴 
            curs.execute(sql)                                                       #sql문 실행
            rs = curs.fetchone()                                                    #한 데이터만  가져옴 
            today = datetime.today().strftime('%Y-%m-%d')                           #오늘 날짜 불러옴 strftime : 날짜 및 시간을 스트링으로 변환
            
            
            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:                 #업데이트가 안된 경우 
                CompanyInfos = self.read_comp_info()                                #상장 목록 파일 불러오기 
                
                for idx in range(len(CompanyInfos)):
                    code = CompanyInfos.code.values[idx]                            #각각의 종목 코드 로드 
                    company = CompanyInfos.company.values[idx]                      #각각의 회사명 로드 
                    sql =  f"REPLACE INTO company_info (code, company, last_update) VALUES ('{code}','{company}','{today}')"         #각 데이터를 업데이트(or insert)하라는 sql문
                    curs.execute(sql)                                                #sql문 실행 
                    self.codes[code] = company                                       #딕셔너리 업데이트 
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                #현재 시간 로드 
                    print(f"[{tmnow}] #{idx+1:04d} REPLACE INTO company_info VALUES ({code}, {company}, {today})")                      #업데이트 완료문 출력 
                
                self.conn.commit()                                   #DB 최종 저장  
    
    def read_naver_sise_info(self, code, company, pages_to_fetch): #1년치 일별 시세 로드
        """네이버에서 주식 시세를 읽어서 데이터프레임으로 반환"""
        try:
            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"         #네이버 금융 사이트 불러오기 
            lastpage = 124                                                          #lastpage를 124로 지정 (최대 5년치 일별 시세 데이터 저장)
            
            df = pd.DataFrame()                                                     #새로운 데이터프레임 생성
            pages = min(int(lastpage), pages_to_fetch)                              #lastpage와 pages_to_fetch(함수의 원소) 중 더 작은 값을 pages로 저장     

            for page in range(1, pages + 1):                                        #페이지 수 만큼 반복
                pg_url = '{}&page={}'.format(url, page)                             #page별 url따기 -> format : 대괄호에 들어갈 값을 연결     
                df = df.append(pd.read_html(requests.get(pg_url, headers = {'User-agent' : 'Mozilla/5.0'}).text)[0])                   #각 page를 html파일을 df에 저장
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                   #현재 시간 저장 
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.format(tmnow, company, code, page, pages), end="\r")      #현재 시간에 어디까지 다운로드 했는지 저장
                                                                                                                         #end : 마지막에 출력할 스트링 , "\r" : 커서를 맨 앞으로 보냄 -> 같은 위치에서 출력 반복  
            
            df = df.rename(columns={'날짜':'date','종가':'close','전일비':'diff','시가':'open','고가':'high','저가':'low','거래량':'volume'})   #각 칼럼의 이름을 영문으로 바꿈
            df['date'] = df['date'].replace('.','-')                                                                                          #데이터의 '.'를 '-'로 바꿈
            df = df.dropna()                                                                                                                  #dropna : 결측값 제거, dropna() : 결측값 있는 전체 행 삭제 
            df[['close','diff','open','high','low','volume']] = df[['close','diff','open','high','low','volume']].astype(int)                 #각 값을 int형으로 변경 (DB에 BIGINT형으로 지정했기 때문에)
            df = df[['date','open','high','low','close','diff','volume']]                                                                     #원하는 순서대로 칼럼 재조합   
        
        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            return None
        
        return df  
    
    def replace_into_db(self, df, num, code, company):                              
        """네이버에서 읽어온 주식 시세를 DB에 REPLACE"""
        with self.conn.cursor() as curs:                                            #cursor(db접근 시, 필요) 불러오기 
            for r in df.itertuples():                                               #itertuples(): 튜플 형식으로 한 줄씩 불러옴
                sql = f"REPLACE INTO daily_price VALUES ('{code}','{r.date}','{r.open}','{r.high}','{r.low}','{r.close}','{r.diff}','{r.volume}')" #값을 바꾼다는 sql 문
                curs.execute(sql)                                                   #sql문 실행
            self.conn.commit()                                                      #최종 db 저장 
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_price[OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), num+1, company, code, len(df)))     #출력문    

    def update_daily_price(self, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""
        for idx, code in enumerate(self.codes):                                     #enumerate(): 몇 번째 출력인지(index)를 함께 반환해줌                                 
            df = self.read_naver_sise_info(code, self.codes[code], pages_to_fetch)  #일별 시세 데이터 프레임 반환
            if df is None:                                                          #데이터 없으면 다시 for문 반복            
                continue
            self.replace_into_db(df, idx, code, self.codes[code])                   #db 업데이트 

    def execute_daily(self):
        """실행 즉시 및 매일 오후 5시에 daily_price 테이블 업데이트"""
        self.update_comp_info()                                                     #회사종목 업데이트

        try:                                                                        #실행 (예외처리 시, 사용)
            with open('config.json','r') as in_file:                                #config.json 파일을 읽기 모드로 open --> in_file의 이름으로 
                config = json.load(in_file)                                         #json파일을 읽어 dict타입으로 저장 
                pages_to_fetch = config['pages_to_fetch']                           #pages_to_fetch 요소를 따로 저장 

        except FileNotFoundError:                                                   #예외 발생 시, (파일이 없는 경우)
            with open('config.json','w') as out_file:                               #config.json 파일을 쓰기 모드로 open --> out_file의 이름으로 
                pages_to_fetch = 124                                                #초기 값을 124로 설정 
                config = {'pages_to_fetch' : 1}                                     #config의 데이터 1로 설정 (최초 업데이트한 후에는 1장씩 업데이트 하기 위해)
                json.dump(config, out_file)                                         #config 값을 json파일로 변경하여 out_file에 저장 

        self.update_daily_price(pages_to_fetch)                                     #pages_to_fetch에 맞춰 가격 업데이트

        tmnow = datetime.now()                                                      #현재 년도, 날짜, 시간 저장
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]                   #이번 달의 마지막 날 반환 
        if tmnow.month == 12 and tmnow.day == lastday:                              #이번 년도의 마지막 날 인 경우, 
            tmnext = tmnow.replace(year = tmnow.year+1, month = 1 , day = 1, hour = 17, minute = 0, second = 0)                 #next는 다음 해, 1월 1일 17시로 저장
        elif tmnow.day == lastday:                                                  #이번 달의 마지막 날 인 경우, 
            tmnext = tmnow.replace(month = tmnow.month+1 , day = 1, hour = 17, minute = 0, second = 0)                          #next는 다음 달, 1일 17시로 저장
        else:                                                                       #마지막 달, 마지막 날 모두 아닌 경우, 
            tmnext = tmnow.replace(day = tmnow.day+1, hour = 17, minute = 0, second = 0)                                       #next는 다음 날, 17시로 저장
        tmdiff = tmnext - tmnow                                                     #현재 시간과 다음 시간의 차이 반환
        secs = tmdiff.seconds                                                       #차이 시간 (초) 저장 

        t = Timer(secs, self.execute_daily)                                         #차이 시간 후에  execute_daily 함수 실행
        print("Waiting for next update({}) ...".format(tmnext.strftime('%Y-%m-%d %H:%M')))          #다음 업데이트 시간 알림 출력 
        t.start()                                                                   #타이머 다시 시작 (매 시간마다 실행하기 위해서)


if __name__ == '__main__':
    dbu = Chartdata() 
    dbu.execute_daily()                                                                                
    
    