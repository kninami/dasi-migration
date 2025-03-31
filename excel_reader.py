import os 
import sys 
import pandas as pd
import db_processor

class Helper:
    def __init__(self):
        pass    
    
    def substr_people(self, data):
        if "(" in data:
            return data.split("(")[1].split(")")[0], data.split("(")[1].split(")")[1]
        else:
            return "성명불상", data.strip()
    
    def distribute_address(self, address):
        if "https://" in address:
            return "online"
        else:
            return "offline"
    
    def make_business_json(self, data):
        business_json = {}
        business_json['name'] = data[0]
        business_json['category'] = data[4]
        address = data[6]
        business_json['type'] = self.distribute_address(address)
        if business_json['type'] == "online":
            business_json['url'] = address
        else:
            business_json['address'] = address
        return business_json
    
    def make_case_json(self, data):
        case_json = {}
        case_json['number'] = data[3]
        case_json['agency'] = self.distribute_agency(data[5])
        case_json['office'] = data[5]
        case_json['office_dept'] = data[6]
        case_json['office_tel'] = data[8]
        case_json['officer'] = data[7]
        case_json['memo'] = data[14]
        return case_json
    
    def distribute_agency(self, data):
        if "법원" in data:
            return "court"
        elif "경찰" in data:
            return "police"
        elif "검찰" in data:
            return "prosecutor"
        
class ExcelReader:
    def __init__(self):
        pass
        
    def read_excel_file(self, file_path, sheet_name=None):
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            return df
        except Exception as e:
            print(f"파일을 읽는 중 오류가 발생했습니다: {e}")
            return None
    
    def case_data_to_json(self, file_url, sheet_name=None):
        df = self.read_excel_file(file_url, "2024 고발 처분내역")
        df.fillna("", inplace=True)

        if df is None:
            print("파일을 읽는 중 오류가 발생했습니다.")
            return None
        else:
            excel_data = df.to_numpy()
            data_array = []
            
            prev_case_id = ""
            current_case = None
            
            for row in excel_data:
                case_id = row[3]  # 사건번호
                
                # 처분 정보 생성
                disposition = {
                    "charge": row[9],  # 죄목
                    "charge_detail": row[10], #세부죄목
                    "disposition": row[11],   # 처분결과
                    "disposition_detail": row[12], #처분일자
                    "disposal_date": row[4].strftime("%Y-%m-%d") if isinstance(row[4], pd.Timestamp) else str(row[4]), #처분일자
                    "fine_amount": row[13]
                }
                
                # 피의자 정보 추출
                name = "성명불상" if row[1] == "" else row[1]
                 
                # 같은 사건인지 확인
                if case_id != prev_case_id:
                    # 이전 사건 데이터가 있으면 배열에 추가
                    if current_case is not None:
                        data_array.append(current_case)
                    
                    # 새 사건 초기화
                    current_case = {
                        "case": Helper().make_case_json(row),
                        "persons": [{
                            "business_name": row[0],
                            "name": name,
                            "role": row[2],
                            "dispositions": [disposition]
                        }]
                    }
                else:
                    # 같은 사건의 피의자 정보 처리
                    person_exists = False
                    for person in current_case["persons"]:
                        if person["name"] == name:
                            # 기존 피의자에 처분 추가
                            person["dispositions"].append(disposition)
                            person_exists = True
                            break
                    
                    # 새로운 피의자인 경우
                    if not person_exists:
                        current_case["persons"].append({
                            "business_name": row[0],
                            "name": name,
                            "role": row[2],
                            "dispositions": [disposition]
                        })
                
                # 현재 사건번호 저장
                prev_case_id = case_id
            
            # 마지막 사건 데이터 추가
            if current_case is not None:
                data_array.append(current_case)

            return data_array
            
    
    def accusation_data_to_json(self, file_url, sheet_name=None):
        df = self.read_excel_file(file_url, "2024 고발")
        if df is None:
            print("파일을 읽는 중 오류가 발생했습니다.")
            return None
        else:
            df.fillna("", inplace=True)
            excel_data = df.to_numpy()
            data_array = []
            
            prev_name = ""
            current_data = None
            
            for row in excel_data:
                same_business_flag = prev_name == row[0]
                
                accusation_json = {}
                name, role = Helper().substr_people(row[2])
    
                accusation_json['name'] = name
                accusation_json['role'] = role
                
                # 새로운 비즈니스인 경우
                if not same_business_flag:
                    # 이전 비즈니스 데이터가 있으면 배열에 추가
                    if current_data is not None:
                        data_array.append(current_data)
                    
                    # 새 비즈니스 데이터 초기화
                    current_data = {
                        "business": Helper().make_business_json(row),
                        "accusations": {
                            "accused_at": row[5],
                            "office": row[7],
                            "accused_person":[accusation_json],
                            "charge": {row[3]}
                        }
                    }
                else:
                    # 같은 비즈니스의 다른 고발 사항이면 배열에 추가
                    current_data["accusations"]["accused_person"].append(accusation_json)
                    current_data["accusations"]["charge"].add(row[3])
                
                # 현재 비즈니스 이름 저장
                prev_name = row[0]
            
            # 마지막 비즈니스 데이터 추가
            if current_data is not None:
                data_array.append(current_data)
                
            return data_array


if __name__ == "__main__":
    excel_reader = ExcelReader()
    data = excel_reader.accusation_data_to_json('./dasi_data.xlsx')
    db_processor.DataProcessor().process_accusation_sheet_data(data)