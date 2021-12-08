from logging import info
import pymysql, json, calendar, requests, urllib3, re
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

class Company_data:
    def __init__(self):                             #초기화함수 
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host="localhost", user='root', password = "1234", db = "QUANT",charset="utf8")

        ############ 테이블 생성 ############
        with self.conn.cursor() as curs:           #cursor 연결 (DB 연결 커서)
            sql = """
            CREATE TABLE IF NOT EXISTS company_condition(  
                code VARCHAR(20),

                2016_12NI BIGINT(20),
                2017_12NI BIGINT(20),
                2018_12NI BIGINT(20),
                2019_12NI BIGINT(20),
                2020_12NI BIGINT(20),
                2021_12NI BIGINT(20),

                2016_12ROE NUMERIC(7,2),
                2017_12ROE NUMERIC(7,2),
                2018_12ROE NUMERIC(7,2),
                2019_12ROE NUMERIC(7,2),
                2020_12ROE NUMERIC(7,2),
                2021_12ROE NUMERIC(7,2),

                2016_12PER NUMERIC(7,2),
                2017_12PER NUMERIC(7,2),
                2018_12PER NUMERIC(7,2),
                2019_12PER NUMERIC(7,2),
                2020_12PER NUMERIC(7,2),
                2021_12PER NUMERIC(7,2),

                2016_12PBR NUMERIC(7,2),
                2017_12PBR NUMERIC(7,2),
                2018_12PBR NUMERIC(7,2),
                2019_12PBR NUMERIC(7,2),
                2020_12PBR NUMERIC(7,2),
                2021_12PBR NUMERIC(7,2),

                SI_PER NUMERIC(5,2),
                PRIMARY KEY (code))
            """                                     #재무 테이블 생성 sql문
            curs.execute(sql)  
            self.conn.commit()                      #DB 최종 저장 

            sql = """
            CREATE TABLE IF NOT EXISTS company_condition_rate( 
                code VARCHAR(20),

                NI_rate BIGINT(20),
                ROE_rate NUMERIC(7,2),
                PER_rate NUMERIC(7,2),
                PBR_rate NUMERIC(7,2),
                PER_group_rate NUMERIC(7,2),

                PRIMARY KEY (code))
            """
        ###################################

        self.codes = dict()                         #종목코드를 담을 딕셔너리 생성 

        sql = "SELECT * FROM company_info"                                          #company_info를 선택한다는 sql문 
        df = pd.read_sql(sql, self.conn)                                            #테이블을 읽어서 df로 변환
       
        for idx in range(len(df)):
           self.codes[df['code'].values[idx]] = df['company'].values[idx]          #종목코드 딕셔너리에 종목코드와 회사 연결해서 저장 
       
    def __del__(self):                              #소멸자함수
        """소멸자: MariaDB 연결 해제 """
        self.conn.close()
                                                                               
    def read_company_condition(self, code):         #종목 재무 현황 크롤링 함수
        
        try:
            ###### 재무 데이터 수집 ######
            url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}"
            html_code = BeautifulSoup(requests.get(url, verify=False, headers = {'User-agent' : 'Mozilla/5.0'}).text,"lxml")   #요청한 url 가져옴, parser --> lxml (html을 해석하는 패키지)

            items = []                                             
            section1 = html_code.find('div', id="highlight_D_Y")   # id 기준으로 크롤링 - 연결재무정보 가져오기
            tds1 = section1.findAll('td')

            print(code)

            for td in tds1:
                td_item = td.get_text()                     
                
                td_item = td_item.replace('\n','')
                td_item = td_item.replace('\t','')
                td_item = td_item.replace('\xa0','') 
                td_item = td_item.replace('N/A','-999') 

                if not td_item :                                    #공백 데이터 인 경우, 데이터 변경 
                    td_item = '-999'
                
                items.append(td_item)

            ###### 동일 업종 PER ######
            url = f"https://finance.naver.com/item/main.naver?"
            code_url = '{}code={}'.format(url, code)                       #url 생성
            html_code = BeautifulSoup(requests.get(code_url, verify=False, headers = {'User-agent' : 'Mozilla/5.0'}).text,"lxml")   #요청한 url 가져옴, parser --> lxml (html을 해석하는 패키지)

            section3 = html_code.findAll("div",class_="gray")[1]              #동일업종 PER
            em = section3.find("em").get_text()

            ###### 데이터프레임 변환 ######
            df = pd.DataFrame({
                'NI2016_12' : items[24],
                'NI2017_12' : items[25],
                'NI2018_12' : items[26],
                'NI2019_12' : items[27],
                'NI2020_12' : items[28],
                'NI2021_12' : items[29],

                'ROE2016_12' : items[136],
                'ROE2017_12' : items[137],
                'ROE2018_12' : items[138],
                'ROE2019_12' : items[139],
                'ROE2020_12' : items[140],
                'ROE2021_12' : items[141],

                'PER2016_12' : items[168],
                'PER2017_12' : items[169],
                'PER2018_12' : items[170],
                'PER2019_12' : items[171],
                'PER2020_12' : items[172],
                'PER2021_12' : items[173],

                'PBR2016_12' : items[176],
                'PBR2017_12' : items[177],
                'PBR2018_12' : items[178],
                'PBR2019_12' : items[179],
                'PBR2020_12' : items[180],
                'PBR2021_12' : items[181],
                
                'SI_PER' : em
                }, index = [0])

            ###### 데이터 실수화 ######
            df['NI2016_12'] = df['NI2016_12'].str.replace(pat='[,]', repl='', regex=True)
            df['NI2017_12'] = df['NI2017_12'].str.replace(pat='[,]', repl='', regex=True)
            df['NI2018_12'] = df['NI2018_12'].str.replace(pat='[,]', repl='', regex=True)
            df['NI2019_12'] = df['NI2019_12'].str.replace(pat='[,]', repl='', regex=True)
            df['NI2020_12'] = df['NI2020_12'].str.replace(pat='[,]', repl='', regex=True)
            df['NI2021_12'] = df['NI2021_12'].str.replace(pat='[,]', repl='', regex=True)

            df[['NI2016_12','NI2017_12','NI2018_12','NI2019_12','NI2020_12','NI2021_12']] = df[['NI2016_12','NI2017_12','NI2018_12','NI2019_12','NI2020_12','NI2021_12']].astype(int)
            df[['ROE2016_12','ROE2017_12','ROE2018_12','ROE2019_12','ROE2020_12','ROE2021_12','PER2016_12','PER2017_12','PER2018_12','PER2019_12','PER2020_12','PER2021_12','PBR2016_12','PBR2017_12','PBR2018_12','PBR2019_12','PBR2020_12','PBR2021_12','SI_PER']] = df[['ROE2016_12','ROE2017_12','ROE2018_12','ROE2019_12','ROE2020_12','ROE2021_12','PER2016_12','PER2017_12','PER2018_12','PER2019_12','PER2020_12','PER2021_12','PBR2016_12','PBR2017_12','PBR2018_12','PBR2019_12','PBR2020_12','PBR2021_12','SI_PER']].astype(float)

            return df

        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            print(self.codes[code])
            return None
    
    def update_company_condition(self):             #종목 재무 현황 DB 저장 함수
        try:
            for idx, code in enumerate(self.codes):                            #페이지 수 만큼 반복
                df = self.read_company_condition(code)                         #일별 시세 데이터 프레임 반환
                if df is None:                                                 #데이터 없으면 다시 for문 반복            
                    continue
                with self.conn.cursor() as curs:                               #cursor(db접근 시, 필요) 불러오기 
                    for r in df.itertuples():                                  #itertuples(): 튜플 형식으로 한 줄씩 불러옴
                        sql = f"REPLACE INTO company_condition VALUES ('{code}','{r.NI2016_12}','{r.NI2017_12}','{r.NI2018_12}','{r.NI2019_12}','{r.NI2020_12}','{r.NI2021_12}','{r.ROE2016_12}','{r.ROE2017_12}','{r.ROE2018_12}','{r.ROE2019_12}','{r.ROE2020_12}','{r.ROE2021_12}','{r.PER2016_12}','{r.PER2017_12}','{r.PER2018_12}','{r.PER2019_12}','{r.PER2020_12}','{r.PER2021_12}','{r.PBR2016_12}','{r.PBR2017_12}','{r.PBR2018_12}','{r.PBR2019_12}','{r.PBR2020_12}','{r.PBR2021_12}','{r.SI_PER}')" #값을 바꾼다는 sql 문
                        curs.execute(sql)                                      #sql문 실행
                        self.conn.commit()                                     #최종 db 저장 
                        print('[{}] #{:04d} ({}) : {} rows > REPLACE INTO company_condition'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), idx, code, len(df)))     #출력문
                                          
        except Exception as e:                                                 #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            print(self.codes[code])

    def processing_rate_data(self):                 #rate 전처리 함수  
        try:
            sql = "SELECT * FROM company_condition"                     #company_condition 선택한다는 sql문 
            df = pd.read_sql(sql, self.conn)   

            for idx in range(len(df)):                                  #코드 수 만큼 반복
                code = df['code'][idx]

                NI_rate_2021 = int(((df['2021_12NI'][idx]-df['2020_12NI'][idx] ) /  abs(df['2020_12NI'][idx]))*100)                   #2021년 당기순이익 작년 대비 증감률
                NI_rate_2020 = int(((df['2020_12NI'][idx]-df['2019_12NI'][idx] ) /  abs(df['2019_12NI'][idx]))*100)                   #2020년 당기순이익 작년 대비 증감률
                NI_rate_2019 = int(((df['2019_12NI'][idx]-df['2018_12NI'][idx] ) /  abs(df['2018_12NI'][idx]))*100)                   #2019년 당기순이익 작년 대비 증감률

                ROE_rate_2021 = float (((df['2021_12ROE'][idx]-df['2020_12ROE'][idx] ) /  abs(df['2020_12ROE'][idx]))*100)            #2021년 ROE 작년 대비 증감률
                ROE_rate_2020 = float (((df['2020_12ROE'][idx]-df['2019_12ROE'][idx] ) /  abs(df['2019_12ROE'][idx]))*100)            #2020년 ROE 작년 대비 증감률
                ROE_rate_2019 = float (((df['2019_12ROE'][idx]-df['2018_12ROE'][idx] ) /  abs(df['2018_12ROE'][idx]))*100)            #2019년 ROE 작년 대비 증감률

                ROE = float (df['2021_12ROE'][idx])  
                PER = float (df['2021_12PER'][idx])                                                                             # 예상 PER
                PBR = float (df['2021_12PBR'][idx])                                                                             # 예상 PBR
                PER_group_rate = float (((df['2021_12PER'][idx]-df['SI_PER'][idx] ) / abs(df['SI_PER'][idx]))*100)                   # 동종업계 대비 PER 계산 

                ### 재무 안정성 추구형에 필요한 데이터 ###
                if (df['2021_12NI'][idx] > 0 and df['2020_12NI'][idx] > 0 and df['2019_12NI'][idx] > 0) :                                      #최근 3년간 당기순이익이 +인 경우
                    NI_3years = 1                                                                                               # 1
                else :                                                                                                          #최근 3년간 당기순이익이 -인 경우
                    NI_3years = 0                                                                                               # 0 

                ### 성장성 추구형에 필요한 데이터 ###
                if (NI_rate_2021 > 0 and NI_rate_2020 > 0 and NI_rate_2019 > 0) :                                               #최근 3년 연속 당기순이익이 증가한 경우
                    NI_3years_rate = 1
                else :
                    NI_3years_rate = 0
                
                if (ROE_rate_2021 > 0 and ROE_rate_2020 > 0 and ROE_rate_2019 > 0):                                             #최근 3년 연속 ROE가 증가한 경우 
                    ROE_3years_rate = 1
                else : 
                    ROE_3years_rate = 0


                with self.conn.cursor() as curs:                               #cursor(db접근 시, 필요) 불러오기 
                    sql = f"REPLACE INTO company_condition_rate VALUES ('{code}','{NI_rate_2021}','{NI_3years}','{NI_3years_rate}','{ROE}','{ROE_rate_2021}','{ROE_3years_rate}','{PER}','{PBR}','{PER_group_rate}')" #값을 바꾼다는 sql 문
                    curs.execute(sql)                                          #sql문 실행
                    self.conn.commit()                                         #최종 db 저장 
                    print('[{}] #{:04d} ({}) : REPLACE INTO company_condition_rate'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), idx, code))     #출력문

        except Exception as e:                                         #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            print(code)


if __name__ == '__main__':
    dbu = Company_data()
    #dbu.update_company_condition()              #재무 정보 업데이트 함수 
    dbu.processing_rate_data()
    

