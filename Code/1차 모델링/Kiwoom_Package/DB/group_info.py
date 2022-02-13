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

class group_data:
    def __init__(self):                             #초기화함수 
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        
        self.conn = pymysql.connect(host="localhost", user='root', password = "1234", db = "QUANT",charset="utf8")

        self.codes = dict()                         #종목코드를 담을 딕셔너리 생성 

        sql = "SELECT * FROM company_info"                                          #company_info를 선택한다는 sql문  
        df = pd.read_sql(sql, self.conn)                                            #테이블을 읽어서 df로 변환

        for idx in range(len(df)):
           self.codes[df['code'].values[idx]] = df['company'].values[idx]          #종목코드 딕셔너리에 종목코드와 회사 연결해서 저장 
    

    def read_naver_group_name(self): #group name 크롤링
        """네이버에서 주식 시세를 읽어서 데이터프레임으로 반환"""
        try:
            for idx, code in enumerate(self.codes):                                     #enumerate(): 몇 번째 출력인지(index)를 함께 반환해줌                                 
               
                url = f"https://finance.naver.com/item/main.naver?code={code}"         #네이버 금융 사이트 불러오기

                html = BeautifulSoup(requests.get(url, verify =False, headers = {'User-agent' : 'Mozilla/5.0'}).text,"lxml")          #각 page를 html파일을 df에 저장
                group = html.find("h4", class_="h_sub sub_tit7")
                group_name = group.find("a")
                group_info = group_name.get_text()

                print(group_info)
                print('#{:04d} {} ({}) : group_name are downloading...'.format(idx, self.codes[code], code))                            #어디까지 다운로드 했는지 저장
                
                with self.conn.cursor() as curs: 
                    sql = f"UPDATE company_info SET company_group = '{group_info}' WHERE code = '{code}'" #값을 바꾼다는 sql 문
                    curs.execute(sql)                                                  #sql문 실행
                    self.conn.commit()                                                 #최종 db 저장                                                                                                                                   #end : 마지막에 출력할 스트링 , "\r" : 커서를 맨 앞으로 보냄 -> 같은 위치에서 출력 반복  
        
        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            return None
        
if __name__ == '__main__':
    dbu = group_data() 
    dbu.read_naver_group_name()                                                                                
    
    