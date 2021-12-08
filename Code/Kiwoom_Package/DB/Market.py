from logging import info
import pymysql,  requests, urllib3
import pandas as pd
#from bs4 import BeautifulSoup
from datetime import datetime
#from threading import Timer, json, calendar,

urllib3.disable_warnings()

"""
DB명 : Quant
시장 반영 데이터 
1.유가(WTI) 2. 환율(원/달러) 3.금(국제) 4.구리 5. 금리
"""
class Market:
    def __init__(self):                             #초기화함수 
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host="localhost", user='root', password = "1234", db = "QUANT",charset="utf8")

        ############ 테이블 생성 ############
        with self.conn.cursor() as curs:           #cursor 연결 (DB 연결 커서)
            sql = """
            CREATE TABLE IF NOT EXISTS market_condition(  
                date DATE,      
                wti NUMERIC(4,2),
                gold NUMERIC(6,2),
                exchange_rate NUMERIC(6,2),
                copper NUMERIC(6,2),
                interast_rate NUMERIC(3,2),
                PRIMARY KEY (date))
            """                                     #시황 테이블 생성 sql문
            curs.execute(sql)
            self.conn.commit()                      #DB 최종 저장 
        ###################################

        self.codes = dict()                         #종목코드를 담을 딕셔너리 생성 
        self.update_market_info()                   #시황 정보 업데이트 함수 실행 
    
    def __del__(self):                              #소멸자함수
        """소멸자: MariaDB 연결 해제 """
        self.conn.close()

    def update_market_info(self):
        """네이버에서 시황 정보를 읽어서 데이터프레임으로 반환"""
        ################### 유가 데이터 크롤링 ####################
        print("start")
        try:
            url = f"https://finance.naver.com/marketindex/worldDailyQuote.naver?marketindexCd=OIL_CL&fdtc=2"        #네이버 금융 사이트 불러오기 
            last_page = 109                                                          #lastpage를 109로 지정 (3년치 유가 데이터 저장)
            
            info_df = pd.DataFrame()                                                      #새로운 데이터프레임 생성 (유가 데이터를 저장할)
            #pages = min(int(lastpage), pages_to_fetch)                              #lastpage와 pages_to_fetch(함수의 원소) 중 더 작은 값을 pages로 저장     

            for page in range(1, last_page + 1):                                     #페이지 수 만큼 반복
                pg_url = '{}&page={}'.format(url, page)                              #page별 url따기 -> format : 대괄호에 들어갈 값을 연결     
                info_df = info_df.append(pd.read_html(requests.get(pg_url, headers = {'User-agent' : 'Mozilla/5.0'}).text)[0])                          #각 page를 html파일을 df에 저장
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                    #현재 시간 저장   
                print('[{}] : {:04d}/{:04d} pages are downloading...'.format(tmnow, page, last_page), end="\r")                               #현재 시간에 어디까지 로드했는지 출력
                                                                                                                                              #end : 마지막에 출력할 스트링 , "\r" : 커서를 맨 앞으로 보냄 -> 같은 위치에서 출력 반복  
            
            info_df = info_df.drop(columns=['보내실 때', '받으실 때'], axis=1)                                                                       #칼럼 삭제             
            info_df = info_df.rename(columns={'날짜':'date','파실 때':'WTI'})                                                                        #각 칼럼의 이름을 영문으로 바꿈
                                                                                      #데이터의 '.'를 '-'로 바꿈
                                                                                                              #dropna : 결측값 제거, dropna() : 결측값 있는 전체 행 삭제 
            info_df[['WTI']] = info_df[['WTI']].astype(float)                                                                                       #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에)
                                                                                                                    
            
            print(info_df)
            print("WTI update OK")
            

        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            return None
        
        ################### 국제 금 시세 크롤링 ####################

        try:
            url = f"https://finance.naver.com/marketindex/worldDailyQuote.naver?marketindexCd=CMDT_GC&fdtc=2"        #네이버 금융 사이트 불러오기 
            last_page = 109                                                          #lastpage를 109로 지정 (3년치 유가 데이터 저장)
            
            df = pd.DataFrame()                                                      #새로운 데이터프레임 생성 (유가 데이터를 저장할)   

            for page in range(1, last_page + 1):                                     #페이지 수 만큼 반복
                pg_url = '{}&page={}'.format(url, page)                              #page별 url따기 -> format : 대괄호에 들어갈 값을 연결     
                df = df.append(pd.read_html(requests.get(pg_url, headers = {'User-agent' : 'Mozilla/5.0'}).text)[0])                          #각 page를 html파일을 df에 저장
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                    #현재 시간 저장 
                print('[{}] : {:04d}/{:04d} pages are downloading...'.format(tmnow, page, last_page), end="\r")                               #현재 시간에 어디까지 로드했는지 출력
                                                                                                                                              #end : 마지막에 출력할 스트링 , "\r" : 커서를 맨 앞으로 보냄 -> 같은 위치에서 출력 반복  
                                                                                                                                              #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에)
             
            df = df.drop(columns=['날짜','보내실 때', '받으실 때'], axis=1)                                                                     #불필요한 데이터 삭제
            df = df.rename(columns={'파실 때':'gold'})                                                                                         #데이터 칼럼명 변경
            df = df.reset_index()                                                                                                              #인덱스 정렬
            
            ##########결측 데이터 추가##########  
            new_data = pd.DataFrame([(1,1322.50)],columns=df.columns,index=[(669)])                                                               #2019.01.30 데이터 추가                                                                                                                         #dropna : 결측값 제거, dropna() : 결측값 있는 전체 행 삭제 
            temp1 = df[df.index < 669]
            temp2 = df[df.index >= 669]
            df = temp1.append(new_data).append(temp2).reset_index()
            df = df.iloc[:-1]                                                                                                                   #마지막 데이터 삭제
            ###################################

            df = df.drop(columns=['index','level_0'], axis=1)                                                                                   #칼럼 정리
            info_df = pd.concat([info_df.reset_index(drop=True),df],axis=1)                                                                     #info_df에 추가
            info_df[['gold']] = info_df[['gold']].astype(float)                                                                                 #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에)                                  
                
            print(info_df)                                                                                      
            print("gold update OK")
        
        except Exception as e:                                                                                                                #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))


        ################### 구리 시세 크롤링 ####################

        try:
            url = f"https://finance.naver.com/marketindex/worldDailyQuote.naver?fdtc=2&marketindexCd=CMDT_CDY"        #네이버 금융 사이트 불러오기 
            last_page = 109                                                                                         #lastpage를 75로 지정 (3년치 유가 데이터 저장)
            
            df = pd.DataFrame()                                                      #새로운 데이터프레임 생성 (유가 데이터를 저장할)   

            for page in range(1, last_page + 1):                                     #페이지 수 만큼 반복
                pg_url = '{}&page={}'.format(url, page)                              #page별 url따기 -> format : 대괄호에 들어갈 값을 연결     
                df = df.append(pd.read_html(requests.get(pg_url, headers = {'User-agent' : 'Mozilla/5.0'}).text)[0])                          #각 page를 html파일을 df에 저장
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                    #현재 시간 저장 
                print('[{}] : {:04d}/{:04d} pages are downloading...'.format(tmnow, page, last_page), end="\r")                               #현재 시간에 어디까지 로드했는지 출력
            
            
            df = df.drop(columns=['날짜','보내실 때', '받으실 때'], axis=1)                                                                     #불필요한 데이터 삭제
            df = df.rename(columns={'파실 때':'cooper'})                                                                                        #데이터 칼럼명 변경
            df = df.reset_index()

            ##########결측 데이터 추가##########  
            new_data = pd.DataFrame([(1,6488.00)],columns=df.columns,index=[(614)])                                                               #2019.01.30 데이터 추가                                                                                                                         #dropna : 결측값 제거, dropna() : 결측값 있는 전체 행 삭제 
            temp1 = df[df.index < 614]
            temp2 = df[df.index >= 614]
            df = temp1.append(new_data).append(temp2)
            
            new_data = pd.DataFrame([(1,5931.50)],columns=df.columns,index=[(692)])                                                               #2019.01.30 데이터 추가                                                                                                                         #dropna : 결측값 제거, dropna() : 결측값 있는 전체 행 삭제 
            temp1 = df[df.index < 692]
            temp2 = df[df.index >= 692]
            
            df = temp1.append(new_data).append(temp2).reset_index()
            df = df.iloc[:-2]                                                                                                                   #마지막 데이터 삭제
            ###################################                                                                                                                                  #end : 마지막에 출력할 스트링 , "\r" : 커서를 맨 앞으로 보냄 -> 같은 위치에서 출력 반복  
                                                                                                                                      #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에)  
            df = df.drop(columns=['index','level_0'], axis=1)
            
            info_df = pd.concat([info_df.reset_index(drop=True),df],axis=1)                                                                     #info_df에 추가
            info_df[['cooper']] = info_df[['cooper']].astype(float)                                                                                 #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에)                                  
                
            print(info_df)                                                                                      
            print("cooper update OK")
        
        except Exception as e:                                                                                                                #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
        
        info_df = info_df.set_index('date')
        
        ################### 원/달러 시세 크롤링 ####################

        try:
            url = f"https://finance.naver.com/marketindex/exchangeDailyQuote.naver?marketindexCd=FX_USDKRW"        #네이버 금융 사이트 불러오기 
            last_page = 75                                                                                         #lastpage를 75로 지정 (3년치 유가 데이터 저장)
            
            df = pd.DataFrame()                                                      #새로운 데이터프레임 생성 (유가 데이터를 저장할)   

            for page in range(1, last_page + 1):                                     #페이지 수 만큼 반복
                pg_url = '{}&page={}'.format(url, page)                              #page별 url따기 -> format : 대괄호에 들어갈 값을 연결     
                df = df.append(pd.read_html(requests.get(pg_url, headers = {'User-agent' : 'Mozilla/5.0'}).text)[0])                          #각 page를 html파일을 df에 저장
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                    #현재 시간 저장 
                print('[{}] : {:04d}/{:04d} pages are downloading...'.format(tmnow, page, last_page), end="\r")                               #현재 시간에 어디까지 로드했는지 출력
                                                                                                                                              #end : 마지막에 출력할 스트링 , "\r" : 커서를 맨 앞으로 보냄 -> 같은 위치에서 출력 반복  
                                                                                                                                               #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에)

            df = df.iloc[:-8]
            df = df.drop(columns=['전일대비','현찰','송금','T/C 사실때','외화수표 파실 때'], axis=1)                                              #불필요한 데이터 삭제
            temp1 = pd.DataFrame(df['날짜'].rename(columns={'날짜':'date'}))
            temp2 = pd.DataFrame(df['매매기준율'].rename(columns={'매매기준율':'dollar'}))
            df = pd.concat([temp1,temp2],axis=1) 
            df = df.set_index('date')
            
            info_df = pd.concat([info_df,df],axis=1)                                                                                           #각 값을 float형으로 변경 (DB에 실수형으로 지정했기 때문에)                                    

            print(info_df)                                                                                
            print("dollar update OK")
        
        except Exception as e:                                                                                                                #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))


        return df  

if __name__ == '__main__':
    dbu = Market() 
      
    