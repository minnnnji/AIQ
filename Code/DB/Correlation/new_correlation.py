from logging import info
import pymysql, json, calendar, requests, urllib3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from threading import Timer 

urllib3.disable_warnings()

"""
DB명 : Quant
업종별 대표 기업 79개의 (2020.10.05 ~ 2021.09.30)간의 차트데이터 - group_price
종목코드(code), 날짜(date), 시가(open), 고가(high), 저가(low),  종가(close), 전일비(diff), 거래량(volume)
"""

class Correlationdata:
    def __init__(self):                             #초기화함수 
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host="localhost", user='root', password = "1234", db = "QUANT",charset="utf8")
        
        ############ 테이블 생성 ###########
        with self.conn.cursor() as curs: 
            sql = """
                CREATE TABLE IF NOT EXISTS company_condition_add(  
                    code VARCHAR(20),
                    NI2020_4Q BIGINT(20),
                    PRIMARY KEY (code))
                """                                     #시황 테이블 생성 sql문
            curs.execute(sql)  
            self.conn.commit()                      #DB 최종 저장 
        ###################################

        self.codes = dict()                         #종목코드를 담을 딕셔너리 생성 

        sql = "SELECT * FROM group_info"                                            #group_info를 선택한다는 sql문 
        df = pd.read_sql(sql, self.conn)                                            #테이블을 읽어서 df로 변환
        for idx in range(len(df)):
           self.codes[df['code'].values[idx]] = df['company'].values[idx]          #종목코드 딕셔너리에 종목코드와 회사 연결해서 저장 

    def read_company_condition(self, code):                               #종목 재무 현황 로드 함수
        
        try:
            url = f"https://finance.naver.com/item/main.naver?"
            code_url = '{}code={}'.format(url, code)                       #url 생성
            html_code = BeautifulSoup(requests.get(code_url, verify=False, headers = {'User-agent' : 'Mozilla/5.0'}).text,"lxml")   #요청한 url 가져옴, parser --> lxml (html을 해석하는 패키지)

            section1 = html_code.find("div", class_="section cop_analysis")   #기업 정보
            tds1 = section1.findAll("td")
        
            items = []

            for td in tds1:
                td_item = td.get_text()
            
                td_item = td_item.replace('\n','')
                td_item = td_item.replace('\t','')
                td_item = td_item.replace('\xa0','')

                items.append(td_item)

            new_df = pd.DataFrame({'NI2020_4Q' : items[26]}, index=[0])
            new_df['NI2020_4Q'] = new_df['NI2020_4Q'].str.replace(pat='[,]', repl='', regex=True)
            new_df[['NI2020_4Q']] = new_df[['NI2020_4Q']].astype(int)
            
            return new_df

        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            print(self.codes[code])
            return None
            
    
    def update_company_condition(self):                             #종목 재무 현황 DB 저장

        try:
            for idx, code in enumerate(self.codes):                            #코드 수 만큼 반복
                df = self.read_company_condition(code)  #종목 재무 데이터 프레임 반환
                if df is None:                                                          #데이터 없으면 다시 for문 반복            
                    continue
               
                with self.conn.cursor() as curs:                                            #cursor(db접근 시, 필요) 불러오기 
                    for r in df.itertuples():                                               #itertuples(): 튜플 형식으로 한 줄씩 불러옴
                        sql = f"INSERT INTO company_condition_add VALUES ('{code}','{r.NI2020_4Q}')"
                        # sql = f"REPLACE INTO company_condition VALUES ('{code}','{r.NI2020_12}','{r.NI2021_12}','{r.NI2021_03}','{r.NI2021_06}','{r.NI2021_09}','{r.ROE2020_12}','{r.ROE2021_12}','{r.PER2020_12}','{r.PER2021_12}','{r.PBR2020_12}','{r.PBR2021_12}','{r.F_rate}','{r.SI_PER}')" #값을 바꾼다는 sql 문
                        curs.execute(sql)                                                       #sql문 실행
                        self.conn.commit()                                                      #최종 db 저장 
                        print('[{}] #{:04d} ({}) : {} rows > REPLACE INTO company_condition_add'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), idx, code, len(df)))     #출력문
                                  
        
        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            print(self.codes[code])
            return None
           

if __name__ == '__main__':
    dbu = Correlationdata()
    dbu.update_company_condition()