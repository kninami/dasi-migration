import os 
import sys 
import pandas as pd
import db_processor

def read_excel_file(file_path):
    try:
        df = pd.read_excel(file_path)
        return df
    except Exception as e:
        print(f"파일을 읽는 중 오류가 발생했습니다: {e}")
        return None

def substr_people(data):
    if "(" in data:
        return data.split("(")[1].split(")")[0], data.split("(")[1].split(")")[1]
    else:
        return "성명불상", data.strip()

def distribute_address(address):
    if "https://" in address:
        return "online"
    else:
        return "offline"
    
def make_business_json(data):
    business_json = {}
    business_json['name'] = data[0]
    business_json['category'] = data[4]
    address = data[6]

    business_json['type'] = distribute_address(address)
    if business_json['type'] == "online":
        business_json['url'] = address
    else:
        business_json['address'] = address
    return business_json

def data_to_json(file_url):
    df = read_excel_file(file_url)
    excel_data = df.to_numpy()
    if df is None:
        print("파일을 읽는 중 오류가 발생했습니다.")
        return None
    else:
        data_array = []
        
        prev_name = ""
        current_data = None
        
        for row in excel_data:
            same_business_flag = prev_name == row[0]
            
            accusation_json = {}
            name, role = substr_people(row[2])

            accusation_json['name'] = name
            accusation_json['role'] = role
            
            # 새로운 비즈니스인 경우
            if not same_business_flag:
                # 이전 비즈니스 데이터가 있으면 배열에 추가
                if current_data is not None:
                    data_array.append(current_data)
                
                # 새 비즈니스 데이터 초기화
                current_data = {
                    "business": make_business_json(row),
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
    data = data_to_json('./dasi_data.xlsx')
    db_processor.DbProcessor().process_data(data)