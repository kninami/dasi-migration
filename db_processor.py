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
    
    def get_business_id(self, name: str) -> Optional[str]:
        response = self.supabase.table('businesses').select('id').eq('name', name).execute()
        if len(response.data) == 0:
            return None
        return response.data[0]['id']
    
    def get_business_type(self, type: str, category: str) -> str:
        response = self.supabase.table('business_types').select('id').eq('name', category).execute()
        if len(response.data) == 0:  # 데이터가 없는 경우 새로 생성
            response = self.supabase.table('business_types').insert({'type': type, 'name': category}).execute()
        return response.data[0]['id']

class AccusationProcessor(DbConnector):
    """고발 데이터를 삽입하고 ID를 반환합니다."""
    def insert_accusation_data(self, business_id: str, accused_at: Any, office: str) -> str:
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
        self.supabase.table('accused_person').insert({
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
    
class CaseProcessor(DbConnector):
    def insert_case_data(self, data: Dict[str, Any]) -> str:
        response = self.supabase.table('cases').insert(data).execute()
        return response.data[0]['id']
        
    def insert_case_person_data(self, data):
        response = self.supabase.table('case_person').insert(data).execute()
        return response.data[0]['id']
    
    def insert_case_person_dispositions(self, data):
        response = self.supabase.table('case_person_dispositions').insert(data).execute()
        return response.data[0]['id']

class CommonProcessor(DbConnector):
    def get_charge_id(self, charge: str, detail_name: str) -> str:
        response = self.supabase.table('charge_types').select('id').eq('name', charge).eq('detail_name', detail_name).execute()
        if len(response.data) == 0: 
            response = self.supabase.table('charge_types').insert({'name': charge}).execute()
        return response.data[0]['id']
    
    def get_disposition_id(self, disposition: str, detail_name: str) -> str:
        response = self.supabase.table('disposition_types').select('id').eq('name', disposition).eq('detail_name', detail_name).execute()
        if len(response.data) == 0: 
            print(f"Disposition not found: {disposition} {detail_name}")
            return None
        return response.data[0]['id']

class DataProcessor:
    def __init__(self):
        self.business_processor = BusinessProcessor()
        self.accusation_processor = AccusationProcessor()
        self.case_processor = CaseProcessor()
        self.common_processor = CommonProcessor()
    
    def process_persons_data(self, data_array: List[Dict[str, Any]]) -> bool:
        success = True
        
        try:
            # person 데이터 삽입 
            for data in data_array:
                if 'business_name' not in data:
                    success = False
                    print(f"Missing business_name in data: {data}")
                    continue
                    
                person_insert = {k: v for k, v in data.items() if k != 'business_name' and k != 'dispositions'}
                business_id = self.business_processor.get_business_id(data['business_name'])
                
                if not business_id:
                    business_id = self.business_processor.insert_business_data({'name': data['business_name']})
                
                person_insert['business_id'] = business_id
                person_id = self.case_processor.insert_case_person_data(person_insert)
                
                # disposition 데이터 삽입
                if 'dispositions' not in data or not data['dispositions']:
                    print(f"No dispositions for person with business: {data['business_name']}")
                    continue
                    
                for disposition in data['dispositions']:
                    charge_id = self.common_processor.get_charge_id(disposition["charge"], disposition["charge_detail"])
                    disposition_id = self.common_processor.get_disposition_id(disposition["disposition"], disposition["disposition_detail"])
                    
                    disposition_insert = {}
                    disposition_insert['fine_amount'] = disposition["fine_amount"] if disposition["fine_amount"] else 0
                    disposition_insert['person_id'] = person_id
                    disposition_insert['disposal_date'] = disposition["disposal_date"]
                    
                    if charge_id and disposition_id:
                        disposition_insert['charge_id'] = charge_id
                        disposition_insert['disposition_id'] = disposition_id
                    elif not charge_id:
                        print(f"Person ID: {person_id} have no charge: {disposition['charge']} {disposition["charge_detail"]}")
                        success = False
                    elif not disposition_id:
                        print(f"Person ID: {person_id} have no disposition: {disposition['disposition']} {disposition["disposition_detail"]}")
                        success = False
                    
                    try:
                        self.case_processor.insert_case_person_dispositions(disposition_insert)
                    except Exception as e:
                        print(f"Error processing disposition data: {e}")
                        
        except Exception as e:
            print(f"Error processing persons data: {e}")
            success = False
        
        return success
    
    def process_case_sheet_data(self, data_array: List[Dict[str, Any]]) -> bool:
        person_array = []
        
        for data in data_array:
            case_data = data['case']
            person_data = data['persons']
                        
            try:
                case_insert = {k: v for k, v in case_data.items() if k != 'business_name'}
                case_id = self.case_processor.insert_case_data(case_insert)
                
                for person in person_data:
                    person['case_id'] = case_id
                    person_array.append(person)
                    
            except Exception as e:
                print(f"Error processing business data: {e}")
        
        if person_array:
            result = self.process_persons_data(person_array)
            return result
        
        return False
    
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
                    for name in charge:
                        charge_id = self.common_processor.get_charge_id(name, None)
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
    