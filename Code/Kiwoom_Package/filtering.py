import pymysql
import pandas as pd

class CompanySelection():

    def __init__(self):                             #초기화함수 
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host="localhost", user='root', password = "1234", db = "QUANT",charset="utf8")

        self.codes = dict()                                                         #종목코드를 담을 딕셔너리 생성 

        sql = "SELECT * FROM company_info"                                          #company_condition를 선택한다는 sql문 
        df = pd.read_sql(sql, self.conn)                                            #테이블을 읽어서 df로 변환
        
        for idx in range(len(df)):
           self.codes[df['code'].values[idx]] = df['company'].values[idx]          #종목코드 딕셔너리에 종목코드와 회사 연결해서 저장 

    def company_selection(self,df,roe,per,pbr):         #조건에 맞는 회사 선별 함수
        """사용자가 입력한 조건에 맞는 기업을 리턴해줍니다."""

        result_code = pd.DataFrame(columns = ['code'])

        try:
            cnt = 0
            for idx in range(len(df)):                                              #코드 수 만큼 반복
                if roe < df['ROE'][idx] and pbr > df['PBR'][idx] and  per > df['PER'][idx] :
                    result_code.loc[cnt,'code'] = df['code'][idx]
                    cnt+=1
                    print('#{:04d} ({})은 조건을 만족합니다.'.format(cnt, df['code'][idx]))


            if result_code is None:
                print("해당 기업이 존재하지 않습니다.\n")
            else:
                print("This is result\n", result_code)

        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            return None

    def Financial_stability_type_selection(self,df):    #재무안정형 기업 선별 함수
        result_code = pd.DataFrame(columns = ['code'])

        try:
            cnt = 0
            for idx in range(len(df)):                                              #코드 수 만큼 반복
                if df['NI_3years'][idx] == 1 and df['PBR'][idx] > 0.7 and df['PBR'][idx] <= 3 and  df['PER_group_rate'][idx] < 0 :
                    result_code.loc[cnt,'code'] = df['code'][idx]
                    cnt+=1
                    print('#{:04d} ({})은 조건을 만족합니다.'.format(cnt, df['code'][idx]))

            if result_code is None:
                print("해당 기업이 존재하지 않습니다.\n")

            else:
                print("This is result\n", result_code)

        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            return None

    def stable_growth_type_selection(self,df):          #안정 성장형 선별 함수
        result_code = pd.DataFrame(columns = ['code'])

        try:
            cnt = 0
            for idx in range(len(df)):                                              #코드 수 만큼 반복
                if df['ROE_3years_rate'][idx] == 1 and df['PBR'][idx] > 0.7 and df['PBR'][idx] <= 3 and  df['PER_group_rate'][idx] < 0 :
                    result_code.loc[cnt,'code'] = df['code'][idx]
                    cnt+=1
                    print('#{:04d} ({})은 조건을 만족합니다.'.format(cnt, df['code'][idx]))

            if result_code is None:
                print("해당 기업이 존재하지 않습니다.\n")

            else:
                print("This is result\n", result_code)

        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            return None
    
    def rapid_growth_type_selection(self,df):           #급 성장형 기업 선별 함수
        result_code = pd.DataFrame(columns = ['code'])

        try:
            cnt = 0
            for idx in range(len(df)):                                              #코드 수 만큼 반복
                if df['ROE_rate_2021'][idx] > 0 and df['PBR'][idx] > 0.7 and df['PBR'][idx] <= 5 and  df['PER_group_rate'][idx] < 0 :
                    result_code.loc[cnt,'code'] = df['code'][idx]
                    cnt+=1
                    print('#{:04d} ({})은 조건을 만족합니다.'.format(cnt, df['code'][idx]))     #출력문

            if result_code is None:
                print("해당 기업이 존재하지 않습니다.\n")

            else:
               print("This is result\n", result_code)

        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            return None

    def Enter_company_info(self):                       #정보 입력 함수
        
        sql = "SELECT * FROM company_condition_rate"                                #company_condition_rate를 선택한다는 sql문 
        df = pd.read_sql(sql, self.conn)                                            #테이블을 읽어서 df로 변환

        print("안녕하세요! AIQ 입니다.")
        print("사용자 님께서 원하시는 기업 유형을 알려주세요!\n")

        print("1: 재무안정형 (꾸준한 이익을 내는 기업)")
        print("2: 안정 성장형 (최근 3년 연속 당기순이익이 증가한 기업)")
        print("3: 급 성장형 (최근 1년 당기순이익이 증가한 기업)")
        print("4: 사용자입력 (해당 ROE,PER,PBR을 가진 기업)\n")

        default = int(input("유형 입력 : "))
        
        if default == 1:
            print("재무안정형 기업을 선택하셨습니다.\n")
            self.Financial_stability_type_selection(df)

        elif default == 2:
            print("안정 성장형 기업을 선택하셨습니다.\n")
            self.stable_growth_type_selection(df)

        elif default == 3:
            print("급 성장형 기업을 선택하셨습니다.\n")
            self.rapid_growth_type_selection(df)

        elif default == 4 :     
            print("사용자 입력을 선택하셨습니다.\n")
            roe = float(input("최소 기준 ROE를 입력하세요 : "))
            per = float(input("최대 기준 PER을 입력하세요 : "))
            pbr = float(input("최대 기준 PBR을 입력하세요 : "))
            print()

            self.company_selection(df,roe,per,pbr)                                                      #default 값을 입력할 경우

        else:
            print("ERROR ! 잘못된 번호 입력.")

if __name__ == '__main__':
    dbu = CompanySelection()
    dbu.Enter_company_info()