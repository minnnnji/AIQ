from logging import info
import pymysql, json, calendar, requests, urllib3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from threading import Timer 
import numpy as np
urllib3.disable_warnings()

"""
DB명 : Quant
시가총액 코스피/닥 100위 기업 재무 데이터 
"""

class Correlationdata:
    def __init__(self):                             #초기화함수 
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host="localhost", user='root', password = "1234", db = "QUANT",charset="utf8")

        ############ 테이블 생성 ############
        with self.conn.cursor() as curs:           #cursor 연결 (DB 연결 커서)
     
            sql = """
            CREATE TABLE IF NOT EXISTS kospi (
                date DATE,
                close NUMERIC(6,2),
                rate NUMERIC(3,2),
                PRIMARY KEY (date))
            """                                     #kospi 흐름 테이블 생성 sql문 
            curs.execute(sql)

            sql = """
            CREATE TABLE IF NOT EXISTS market_condition(  
                date DATE,      
                wti NUMERIC(4,2),
                wti_rate NUMERIC(5,2),
                gold NUMERIC(5,1),
                gold_rate NUMERIC(3,2),
                copper NUMERIC(5,1),
                cooper_rate NUMERIC(3,2),
                dollar NUMERIC(5,1),
                PRIMARY KEY (date))
            """                                     #시황 테이블 생성 sql문
            curs.execute(sql)                       #sql문 실행 
            
            sql = """
            CREATE TABLE IF NOT EXISTS company_condition(  
                code VARCHAR(20),
                2020_12NI BIGINT(20),
                2021_12NI BIGINT(20),
                2021_03NI BIGINT(20),
                2021_06NI BIGINT(20),
                2021_09NI BIGINT(20),
                2020_12ROE NUMERIC(5,2),
                2021_12ROE NUMERIC(5,2),
                2020_12PER NUMERIC(5,2),
                2021_12PER NUMERIC(5,2),
                2020_12PBR NUMERIC(5,2),
                2021_12PBR NUMERIC(5,2),
                F_rate NUMERIC(3,1),
                SI_PER NUMERIC(5,2),
                PRIMARY KEY (code))
            """                                     #재무 테이블 생성 sql문
            curs.execute(sql)  
            self.conn.commit()                      #DB 최종 저장 
        ###################################

        self.codes = dict()                         #종목코드를 담을 딕셔너리 생성 

        sql = "SELECT * FROM company_info"                                          #company_info를 선택한다는 sql문 
        df = pd.read_sql(sql, self.conn)                                            #테이블을 읽어서 df로 변환
        for idx in range(len(df)):
           self.codes[df['code'].values[idx]] = df['company'].values[idx]          #종목코드 딕셔너리에 종목코드와 회사 연결해서 저장 
       
    def __del__(self):                              #소멸자함수
        """소멸자: MariaDB 연결 해제 """
        self.conn.close()
                                                                               
    def update_kospi(self):                                         #코스피 로드 및 DB 저장 함수
        """네이버에서 코스피 지수를 읽어서 데이터 프레임으로 반환"""

        url = f'https://finance.naver.com/sise/sise_index_day.naver?code=KOSPI'
        pages = 208

        df = pd.DataFrame()                                                     #새로운 데이터프레임 생성

        for page in range(1, pages + 1):                                        #페이지 수 만큼 반복
            pg_url = '{}&page={}'.format(url, page)                             #page별 url따기 -> format : 대괄호에 들어갈 값을 매칭     
            df = df.append(pd.read_html(requests.get(pg_url, headers = {'User-agent' : 'Mozilla/5.0'}).text)[0])                    #각 page를 html파일을 df에 저장
            tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                   #현재 시간 저장 
            print('[{}] : {:04d}/{:04d} pages are downloading...'.format(tmnow, page, pages), end="\r")      #현재 시간에 어디까지 다운로드 했는지 저장
             
        df = df.drop(columns=['전일비','거래량(천주)','거래대금(백만)'], axis=1)
        df = df.rename(columns={'날짜':'date','체결가':'close','등락률':'rate'})                                                            #각 칼럼의 이름을 영문으로 바꿈
        df = df.dropna() 
                                                                                        
        df['rate'] = df['rate'].str.replace(pat='[%]', repl='', regex=True)                                                                                                                  #dropna : 결측값 제거, dropna() : 결측값 있는 전체 행 삭제 
        df[['close']] = df[['close']].astype(float)                                                                                         #각 값을 int형으로 변경 (DB에 BIGINT형으로 지정했기 때문에)
        df[['rate']] = df[['rate']].astype(float) 
                                                                         
        df = df.iloc[1:-4]
        print(df) 

        with self.conn.cursor() as curs:                                            #cursor(db접근 시, 필요) 불러오기 
            for r in df.itertuples():                                               #itertuples(): 튜플 형식으로 한 줄씩 불러옴
                sql = f"REPLACE INTO kospi VALUES ('{r.date}','{r.close}','{r.rate}')" #값을 바꾼다는 sql 문
                curs.execute(sql)                                                   #sql문 실행
            self.conn.commit()                                                      #최종 db 저장 
            print('[{}] : {} rows > REPLACE INTO kospi [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), len(df)))     #출력문    
        
        return df

    def update_market_condition(self):                              #시장 흐름 데이터 DB 저장 함수
        ################### 유가 데이터 크롤링 ####################
        try:
            url = f"https://finance.naver.com/marketindex/worldDailyQuote.naver?marketindexCd=OIL_CL&fdtc=2"        #네이버 금융 사이트 불러오기 
            last_page = 182                                                           #lastpage를 109로 지정 (3년치 유가 데이터 저장)
            
            info_df = pd.DataFrame()                                                 #새로운 데이터프레임 생성 (유가 데이터를 저장할)

            for page in range(1, last_page + 1):                                     #페이지 수 만큼 반복
                pg_url = '{}&page={}'.format(url, page)                              #page별 url따기 -> format : 대괄호에 들어갈 값을 연결     
                info_df = info_df.append(pd.read_html(requests.get(pg_url, headers = {'User-agent' : 'Mozilla/5.0'}).text)[0])                          #각 page를 html파일을 df에 저장
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                    #현재 시간 저장   
                print('[{}] : {:04d}/{:04d} pages are downloading...'.format(tmnow, page, last_page), end="\r")                               #현재 시간에 어디까지 로드했는지 출력
                                                                                                                                              #end : 마지막에 출력할 스트링 , "\r" : 커서를 맨 앞으로 보냄 -> 같은 위치에서 출력 반복  
            
            info_df = info_df.drop(columns=['보내실 때','받으실 때'], axis=1)                                                                   #칼럼 삭제             
            info_df = info_df.rename(columns={'날짜':'date','파실 때':'WTI'})                                                                   #각 칼럼의 이름을 영문으로 바꿈
            
            info_df['date'] = info_df['date'].replace('.','-')                                                                                 #데이터의 '.'를 '-'로 바꿈
            info_df = info_df.dropna()                                                                                                         #dropna : 결측값 제거, dropna() : 결측값 있는 전체 행 삭제 

            info_df = info_df.set_index('date')
            info_df[['WTI']] = info_df[['WTI']].astype(float)                                                                                  #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에)
                                                                                        
            print("WTI update OK")
            
        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            return None
        
        ################### 국제 금 시세 크롤링 ####################

        try:
            url = f"https://finance.naver.com/marketindex/worldDailyQuote.naver?marketindexCd=CMDT_GC&fdtc=2"        #네이버 금융 사이트 불러오기 
            last_page = 182                                                          #lastpage를 109로 지정 (3년치 유가 데이터 저장)
            
            df = pd.DataFrame()                                                      #새로운 데이터프레임 생성 (유가 데이터를 저장할)   

            for page in range(1, last_page + 1):                                     #페이지 수 만큼 반복
                pg_url = '{}&page={}'.format(url, page)                              #page별 url따기 -> format : 대괄호에 들어갈 값을 연결     
                df = df.append(pd.read_html(requests.get(pg_url, headers = {'User-agent' : 'Mozilla/5.0'}).text)[0])                          #각 page를 html파일을 df에 저장
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                    #현재 시간 저장 
                print('[{}] : {:04d}/{:04d} pages are downloading...'.format(tmnow, page, last_page), end="\r")                               #현재 시간에 어디까지 로드했는지 출력
                                                                                                                                              #end : 마지막에 출력할 스트링 , "\r" : 커서를 맨 앞으로 보냄 -> 같은 위치에서 출력 반복                                                                                                                                    #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에) 
            df = df.drop(columns=['보내실 때','받으실 때'], axis=1)                                                                                 #불필요한 데이터 삭제
            df = df.rename(columns={'날짜':'date','파실 때':'gold'})                                                                                         #데이터 칼럼명 변경
            
            df = df.set_index('date')
            print(df)

            info_df = pd.concat([info_df,df], axis=1)                                                                   #info_df에 추가
            
            info_df[['gold']] = info_df[['gold']].astype(float)                                                                    #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에)                                  
                                                                                          
            print("gold update OK")
        
        except Exception as e:                                                                                                                #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))

        ################### 구리 시세 크롤링 ####################

        try:
            url = f"https://finance.naver.com/marketindex/worldDailyQuote.naver?fdtc=2&marketindexCd=CMDT_CDY"        #네이버 금융 사이트 불러오기 
            last_page = 182                                                                                           #lastpage를 36로 지정 
            
            df = pd.DataFrame()                                                      #새로운 데이터프레임 생성 (유가 데이터를 저장할)   

            for page in range(1, last_page + 1):                                     #페이지 수 만큼 반복
                pg_url = '{}&page={}'.format(url, page)                              #page별 url따기 -> format : 대괄호에 들어갈 값을 연결     
                df = df.append(pd.read_html(requests.get(pg_url, headers = {'User-agent' : 'Mozilla/5.0'}).text)[0])                          #각 page를 html파일을 df에 저장
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                    #현재 시간 저장 
                print('[{}] : {:04d}/{:04d} pages are downloading...'.format(tmnow, page, last_page), end="\r")                               #현재 시간에 어디까지 로드했는지 출력
            
    
            df = df.drop(columns=['보내실 때','받으실 때'])                                                                     #불필요한 데이터 삭제
            df = df.rename(columns={'날짜':'date','파실 때':'cooper'})                                                                                        #데이터 칼럼명 변경
            
            df = df.set_index('date')                                                                                                                

            info_df = pd.concat([info_df,df], axis=1)                                                                         #info_df에 추가

            info_df[['cooper']] = info_df[['cooper']].astype(float)                                                                                #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에)                                  
                                                                                    
            print("cooper update OK")
        
        except Exception as e:                                                                                                                #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
        
        ################### 원/달러 시세 크롤링 ####################

        try:
            url = f"https://finance.naver.com/marketindex/exchangeDailyQuote.naver?marketindexCd=FX_USDKRW"        #네이버 금융 사이트 불러오기 
            last_page = 125                                                                                       #lastpage를 75로 지정 (3년치 유가 데이터 저장)
            
            df = pd.DataFrame()                                                      #새로운 데이터프레임 생성 (유가 데이터를 저장할)   

            for page in range(1, last_page + 1):                                     #페이지 수 만큼 반복
                pg_url = '{}&page={}'.format(url, page)                              #page별 url따기 -> format : 대괄호에 들어갈 값을 연결     
                df = df.append(pd.read_html(requests.get(pg_url, headers = {'User-agent' : 'Mozilla/5.0'}).text)[0])                          #각 page를 html파일을 df에 저장
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                    #현재 시간 저장 
                print('[{}] : {:04d}/{:04d} pages are downloading...'.format(tmnow, page, last_page), end="\r")                               #현재 시간에 어디까지 로드했는지 출력
                                                                                                                                              #end : 마지막에 출력할 스트링 , "\r" : 커서를 맨 앞으로 보냄 -> 같은 위치에서 출력 반복  
                                                                                                                                              #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에
            
            df = df.drop(columns=['전일대비','현찰','송금','T/C 사실때','외화수표 파실 때'], axis=1)                                              #불필요한 데이터 삭제
            temp1 = pd.DataFrame(df['날짜'].rename(columns={'날짜':'date'}))
            temp2 = pd.DataFrame(df['매매기준율'].rename(columns={'매매기준율':'dollar'}))
            df = pd.concat([temp1,temp2], axis=1) 
            df = df.set_index('date')

            info_df = pd.concat([info_df,df], axis=1)                                                                                            #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에)                                    
            info_df[['dollar']] = info_df[['dollar']].astype(float) 

            info_df = info_df.reset_index()                                                                            
            print("dollar update OK")   
        
        ####################결측값 전처리#######################
            info_df['date']=pd.to_datetime(info_df['date'])
            info_df.sort_values(by=['date'], inplace=True)            #날짜순으로 정렬
            info_df = info_df.reset_index()
            info_df = info_df.drop('index',axis=1)
            info_df = info_df.iloc[:-2]                  #업데이트 x->NaN값 제거
            info_df=info_df.fillna(method='ffill')      #전일가와 동일하게 전처리 

            info_df['WTI'][0] = 48.69
            print(info_df)

            tmp_wti = np.zeros(len(info_df))
            tmp_gold = np.zeros(len(info_df))
            tmp_cooper = np.zeros(len(info_df))
            tmp_dollar = np.zeros(len(info_df))

            first = info_df.index.tolist()[0]
            last = info_df.index.tolist()[-1]

            for i in range(first, last):
                tmp_wti[i+1-first] = round(((info_df['WTI'].loc[i+1]-info_df['WTI'].loc[i])/info_df['WTI'].loc[i]) * 100, 2)
                tmp_gold[i+1-first] = round(((info_df['gold'].loc[i+1]-info_df['gold'].loc[i])/info_df['gold'].loc[i]) * 100, 2)
                tmp_cooper[i+1-first] = round(((info_df['cooper'].loc[i+1]-info_df['cooper'].loc[i])/info_df['cooper'].loc[i]) * 100, 2)
                tmp_dollar[i+1-first] = round(((info_df['dollar'].loc[i+1]-info_df['dollar'].loc[i])/info_df['dollar'].loc[i]) * 100, 2)

            info_df['WTI_rate'] = tmp_wti
            info_df['gold_rate'] = tmp_gold
            info_df['cooper_rate'] = tmp_cooper
            info_df['dollar_rate'] = tmp_dollar

            info_df[['WTI_rate']] = info_df[['WTI_rate']].astype(float)
            info_df[['gold_rate']] = info_df[['gold_rate']].astype(float) 
            info_df[['cooper_rate']] = info_df[['cooper_rate']].astype(float)
            info_df[['dollar_rate']] = info_df[['dollar_rate']].astype(float)
        ##########################################################
            print(info_df)
        #########DB에 저장#########
            with self.conn.cursor() as curs:                                            #cursor(db접근 시, 필요) 불러오기 
                for r in info_df.itertuples():                                          #itertuples(): 튜플 형식으로 한 줄씩 불러옴
                    sql = f"REPLACE INTO market_condition VALUES ('{r.date}','{r.WTI}','{r.WTI_rate}','{r.gold}','{r.gold_rate}','{r.cooper}','{r.cooper_rate}','{r.dollar}','{r.dollar_rate}')" #값을 바꾼다는 sql 문
                    curs.execute(sql)                                                   #sql문 실행
                self.conn.commit()                                                      #최종 db 저장 
                print('[{}] : {} rows > REPLACE INTO market_condition [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), len(df)))     #출력문 

        except Exception as e:                                                                                                                #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))

    def read_company_condition(self, code):                         #종목 재무 현황 로드 함수
        
        try:
            url = f"https://finance.naver.com/item/main.naver?"
            code_url = '{}code={}'.format(url, code)                       #url 생성
            html_code = BeautifulSoup(requests.get(code_url, verify=False, headers = {'User-agent' : 'Mozilla/5.0'}).text,"lxml")   #요청한 url 가져옴, parser --> lxml (html을 해석하는 패키지)

            section1 = html_code.find("div", class_="section cop_analysis")   #기업 정보
            tds1 = section1.findAll("td")
         
            section2 = html_code.find("div", class_="gray")                   #외국인 보유 비율
            tds2 = section2.findAll("td")

            section3 = html_code.findAll("div",class_="gray")[1]              #동일업종 PER
            em = section3.find("em").get_text()
        
            items = []

            for td in tds1:
                td_item = td.get_text()
            
                td_item = td_item.replace('\n','')
                td_item = td_item.replace('\t','')
                td_item = td_item.replace('\xa0','')

                items.append(td_item)
        
            for td in tds2:
                td_item = td.get_text()
            
                items.append(td_item)

            df = pd.DataFrame({
                'NI2020_12' : items[22],
                'NI2021_12' : items[23],
                'NI2021_03' : items[27],
                'NI2021_06' : items[28],
                'NI2021_09' : items[29],
                'ROE2020_12' : items[52],
                'ROE2021_12' : items[53],
                'PER2020_12' : items[102],
                'PER2021_12' : items[103],
                'PBR2020_12' : items[122],
                'PBR2021_12' : items[123],
                'F_rate' : items[-1],
                'SI_PER' : em
                }, index = [0])


            df['F_rate'] = df['F_rate'].str.replace(pat='[%]', repl='', regex=True)
            df['NI2020_12'] = df['NI2020_12'].str.replace(pat='[,]', repl='', regex=True)
            df['NI2021_12'] = df['NI2021_12'].str.replace(pat='[,]', repl='', regex=True)
            df['NI2021_03'] = df['NI2021_03'].str.replace(pat='[,]', repl='', regex=True)
            df['NI2021_06'] = df['NI2021_06'].str.replace(pat='[,]', repl='', regex=True)
            df['NI2021_09'] = df['NI2021_09'].str.replace(pat='[,]', repl='', regex=True)

            df[['NI2020_12','NI2021_12','NI2021_03','NI2021_06','NI2021_09']] = df[['NI2020_12','NI2021_12','NI2021_03','NI2021_06','NI2021_09']].astype(int)
            df[['ROE2020_12','ROE2021_12','PER2020_12','PER2021_12','PBR2020_12','PBR2021_12','F_rate','SI_PER']] = df[['ROE2020_12','ROE2021_12','PER2020_12','PER2021_12','PBR2020_12','PBR2021_12','F_rate','SI_PER']].astype(float)
        
            
            return df

        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            print(self.codes[code])
            return None
    
    def update_company_condition(self):                             #종목 재무 현황 DB 저장
        try:
            for idx, code in enumerate(self.codes):                            #페이지 수 만큼 반복
                df = self.read_company_condition(code)                         #일별 시세 데이터 프레임 반환
                if df is None:                                                 #데이터 없으면 다시 for문 반복            
                    continue
                df = self.processing_data(df)                                  #데이터 전처리 함수 
                with self.conn.cursor() as curs:                               #cursor(db접근 시, 필요) 불러오기 
                    for r in df.itertuples():                                               #itertuples(): 튜플 형식으로 한 줄씩 불러옴
                        sql = f"REPLACE INTO company_condition VALUES ('{code}','{r.NI2020_12}','{r.NI2021_12}','{r.NI2021_03}','{r.NI2021_06}','{r.NI2021_09}','{r.ROE2020_12}','{r.ROE2021_12}','{r.PER2020_12}','{r.PER2021_12}','{r.PBR2020_12}','{r.PBR2021_12}','{r.F_rate}','{r.SI_PER}')" #값을 바꾼다는 sql 문
                        curs.execute(sql)                                      #sql문 실행
                        self.conn.commit()                                     #최종 db 저장 
                        print('[{}] #{:04d} ({}) : {} rows > REPLACE INTO company_condition'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), idx, code, len(df)))     #출력문
                                  
        
        except Exception as e:                                                 #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            print(self.codes[code])
            return None    

    def processing_rate_data(self, df):

        df = self.update_market_condition()

        tmp_wti = np.zeros(len(df))
        tmp_gold = np.zeros(len(df))
        tmp_cooper = np.zeros(len(df))
        tmp_dollar = np.zeros(len(df))

        first = df.index.tolist()[0]
        last = df.index.tolist()[-1]

        for i in range(first, last):
            tmp_wti[i+1-first] = round(((df['WTI'].loc[i+1]-df['WTI'].loc[i])/df['WTI'].loc[i]) * 100, 2)
            tmp_gold[i+1-first] = round(((df['gold'].loc[i+1]-df['gold'].loc[i])/df['gold'].loc[i]) * 100, 2)
            tmp_cooper[i+1-first] = round(((df['cooper'].loc[i+1]-df['cooper'].loc[i])/df['cooper'].loc[i]) * 100, 2)
            tmp_dollar[i+1-first] = round(((df['dollar'].loc[i+1]-df['dollar'].loc[i])/df[''].loc[i]) * 100, 2)

        df['WTI_rate'] = tmp_wti
        df['gold_rate'] = tmp_gold
        df['cooper_rate'] = tmp_cooper
        df['dollar_rate'] = tmp_dollar

        with self.conn.cursor() as curs:                                            #cursor(db접근 시, 필요) 불러오기 
            for r in df.itertuples():                                          #itertuples(): 튜플 형식으로 한 줄씩 불러옴
                sql = f"REPLACE INTO market_condition VALUES ('{r.date}','{r.WTI}','{r.WTI_rate}','{r.gold}','{r.gold_rate}','{r.cooper}','{r.cooper_rate}','{r.dollar}')" #값을 바꾼다는 sql 문
                curs.execute(sql)                                                   #sql문 실행
            self.conn.commit()                                                      #최종 db 저장 
            print('[{}] : {} rows > REPLACE INTO market_condition [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), len(df)))     #출력문    


        return df

if __name__ == '__main__':
    dbu = Correlationdata()
    #dbu.update_daily_price()
    #dbu.update_kospi()
    dbu.update_market_condition()
    #dbu.update_company_condition
    

