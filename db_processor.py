import supabase
import os 
import dotenv
from typing import List, Dict, Any, Optional

class DbConnector:
    def __init__(self):
        dotenv.load_dotenv()
        
        self.SUPABASE_URL = os.getenv('SUPABASE_URL')
        self.SUPABASE_KEY = os.getenv('SUPABASE_KEY')        
        self.supabase = supabase.create_client(self.SUPABASE_URL, self.SUPABASE_KEY)

class BusinessProcessor(DbConnector):
    def insert_business_data(self, data: Dict[str, Any]) -> str:
        response = self.supabase.table('businesses').insert(data).execute()
        return response.data[0]['id']
    
    def check_duplicate_business(self, name: str) -> bool:
        response = self.supabase.table('businesses').select('id').eq('name', name).execute()
        return len(response.data) > 0
    
    def get_business_type(self, type: str, category: str) -> str:
        response = self.supabase.table('business_types').select('id').eq('name', category).execute()
        if len(response.data) == 0:  # 데이터가 없는 경우 새로 생성
            response = self.supabase.table('business_types').insert({'type': type, 'name': category}).execute()
        return response.data[0]['id']

class AccusationProcessor(DbConnector):
    """고발 데이터를 삽입하고 ID를 반환합니다."""
    def insert_accusation_data(self, business_id: str, accused_at: Any, office: str) -> str:
        # Timestamp 객체를 문자열로 변환
        if hasattr(accused_at, 'isoformat'):
            accused_at = accused_at.isoformat()    
        response = self.supabase.table('accusations').insert({
            'business_id': business_id, 
            'accused_at': accused_at, 
            'office': office
        }).execute()
        return response.data[0]['id']
    
    """고발된 사람 정보를 삽입합니다."""
    def insert_accused_person(self, accusation_id: str, name: str, role: str) -> None:
        self.supabase.table('accusation_accused_persons').insert({
            'accusation_id': accusation_id, 
            'name': name, 
            'role': role
        }).execute()
    
    """고발 혐의 정보를 삽입합니다."""
    def insert_accusation_charge(self, accusation_id: str, charge_id: str) -> None:
        self.supabase.table('accusation_charges').insert({
            'accusation_id': accusation_id, 
            'charge_id': charge_id
        }).execute()
    
    """혐의 ID를 가져오거나 새로 생성합니다."""
    def get_charge_id(self, charge: str) -> str:
        response = self.supabase.table('charges').select('id').eq('name', charge).is_('detail_name', None).execute()
        if len(response.data) == 0: 
            response = self.supabase.table('charges').insert({'name': charge}).execute()
        return response.data[0]['id']

class DataProcessor:
    def __init__(self):
        self.business_processor = BusinessProcessor()
        self.accusation_processor = AccusationProcessor()
    
    """여러 고발 데이터를 처리합니다."""
    def process_accusation_data(self, data: List[Dict[str, Any]]) -> bool:
        try:
            for accusation in data:
                accusation_id = self.accusation_processor.insert_accusation_data(
                    accusation["business_id"], 
                    accusation["accused_at"], 
                    accusation["office"]
                )
                
                for person in accusation["accused_person"]:
                    self.accusation_processor.insert_accused_person(
                        accusation_id, 
                        person["name"], 
                        person["role"]
                    )
                
                for charge in accusation["charge"]:
                    charge_id = self.accusation_processor.get_charge_id(charge)
                    self.accusation_processor.insert_accusation_charge(accusation_id, charge_id)
            return True
        except Exception as e:
            print(f"Error processing accusation data: {e}")
            return False
    
    """고발 시트에서 가져온 데이터를 처리합니다."""
    def process_accusation_sheet_data(self, data_array: List[Dict[str, Any]]) -> bool:
        accusation_array = []
        
        for data in data_array:
            business_data = data['business']
            accusation_data = data['accusations']
            
            try:
                biz_insert = {k: v for k, v in business_data.items() if k != 'category'}
                biz_insert['business_type_id'] = self.business_processor.get_business_type(
                    business_data['type'], 
                    business_data['category']
                )
                
                business_id = self.business_processor.insert_business_data(biz_insert)
                accusation_data["business_id"] = business_id
                accusation_array.append(accusation_data)
            except Exception as e:
                print(f"Error processing business data: {e}")
        
        if accusation_array:
            return self.process_accusation_data(accusation_array)
        return False
    