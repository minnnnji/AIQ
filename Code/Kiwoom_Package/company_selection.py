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

    def company_default_selection(self):           #default 회사 선별 함수
        """디폴트 조건에 맞는 기업을 리턴해줍니다."""
        sql = "SELECT * FROM company_condition_rate"                               #company_condition를 선택한다는 sql문 
        df = pd.read_sql(sql, self.conn)                                            #테이블을 읽어서 df로 변환

        result_code = pd.DataFrame(columns = ['code'])

        try:
            cnt = 0
            for idx in range(len(df)):                                              #코드 수 만큼 반복

                ## default 값 ##
                roe = df['2021_12PER'][idx] * 3                                     #ROE는 PER의 3배 이상 
                pbr = 5
                #PER은 동종업계 평균 PER 보다 작은 기업 리턴 

                if roe < df['2021_12ROE'][idx] and pbr > df['2021_12PBR'][idx] and df['2021_12PBR'][idx] > 0.3 and  df['2021_12PER'][idx] < df['SI_PER'][idx] :
                    result_code.loc[cnt,'code'] = df['code'][idx]
                    cnt+=1
                    print('{} satisfies the conditions.'.format(df['code'][idx]))


            if result_code is None:
                print("No company meets the conditions.\n")
            else:
                print("This is result\n", result_code)

        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            return None
    
    def company_selection(self,roe,per,pbr):        #조건에 맞는 회사 선별 함수
        """사용자가 입력한 조건에 맞는 기업을 리턴해줍니다."""
        sql = "SELECT * FROM company_condition"                                     #company_condition를 선택한다는 sql문 
        df = pd.read_sql(sql, self.conn)                                            #테이블을 읽어서 df로 변환

        result_code = pd.DataFrame(columns = ['code'])

        try:
            cnt = 0
            for idx in range(len(df)):                                              #코드 수 만큼 반복
                if roe < df['2021_12ROE'][idx] and pbr > df['2021_12PBR'][idx] and  per > df['2021_12PER'][idx] :
                    result_code.loc[cnt,'code'] = df['code'][idx]
                    cnt+=1
                    print('{} satisfies the conditions.'.format(df['code'][idx]))


            if result_code is None:
                print("No company meets the conditions.\n")
            else:
                print("This is result\n", result_code)

        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            return None

    def Enter_company_info(self):                       #정보 입력 함수
        
        print("What are the conditions of the company you want?")

        default = float(input("if you want to enter default of all values, Enter the 0 : "))

        if default == 0 :                                                           #default 값을 입력할 경우
            print("selecting companies that meet the conditions...\n")
            self.company_default_selection()
        else:
            roe = float(input("Enter the ROE Min value : "))
            per = float(input("Enter the PER Max value : "))
            pbr = float(input("Enter the PBR Max value : "))

            print("selecting companies that meet the conditions...\n")
            self.company_selection(roe,per,pbr)

if __name__ == '__main__':
    dbu = CompanySelection()
    dbu.Enter_company_info()