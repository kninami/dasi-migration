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
    
    def get_business_type(self, type: str = None, category: str = None) -> str:
        response = self.supabase.table('business_types').select('id').eq('name', category).execute()
        if len(response.data) == 0:  # 데이터가 없는 경우 새로 생성
            insert_data = {'name': category}
            if type is not None:
                insert_data['type'] = type
            response = self.supabase.table('business_types').insert(insert_data).execute()
        return response.data[0]['id']

class AccusationProcessor(DbConnector):
    def insert_accusation_data(self, business_id: str, accused_at: Any, office: str) -> str:
        if hasattr(accused_at, 'isoformat'):
            accused_at = accused_at.isoformat()    
        response = self.supabase.table('accusations').insert({
            'business_id': business_id, 
            'accused_at': accused_at, 
            'office': office
        }).execute()
        return response.data[0]['id']
    
    def insert_accused_person(self, accusation_id: str, name: str, role: str) -> None:
        self.supabase.table('accused_person').insert({
            'accusation_id': accusation_id, 
            'name': name, 
            'role': role
        }).execute()
    
    def insert_accusation_charge(self, accusation_id: str, charge_id: str) -> None:
        self.supabase.table('accusation_charges').insert({
            'accusation_id': accusation_id, 
            'charge_id': charge_id
        }).execute()

class ReportProcessor(DbConnector):
    def insert_report_data(self, data: Dict[str, Any]) -> str:
        response = self.supabase.table('reports').insert(data).execute()
        return response.data[0]['id']
    
    def insert_report_disposition(self, data: Dict[str, Any]) -> str:
        response = self.supabase.table('report_dispositions').insert(data).execute()
        return response.data[0]['id']
        
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
        query = self.supabase.table('charge_types').select('id').eq('name', charge)
        if detail_name: 
            query = query.eq('detail_name', detail_name)
        response = query.execute()
        if len(response.data) == 0: 
            response = self.supabase.table('charge_types').insert({'name': charge, 'detail_name': detail_name}).execute()
        return response.data[0]['id']
    
    def get_disposition_id(self, disposition: str, detail_name: str) -> str:
        query = self.supabase.table('disposition_types').select('id').eq('name', disposition)
        if detail_name: 
            query = query.eq('detail_name', detail_name)
        response = query.execute()
        return response.data[0]['id']

class DataProcessor:
    def __init__(self):
        self.business_processor = BusinessProcessor()
        self.accusation_processor = AccusationProcessor()
        self.case_processor = CaseProcessor()
        self.common_processor = CommonProcessor()
        self.report_processor = ReportProcessor()
    
    def process_persons_data(self, data_array: List[Dict[str, Any]]) -> bool:
        success = True
        try:
            # person 데이터 삽입 
            for data in data_array:
                print(data)
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
                print(f"Person ID: {person_id} processed successfully")
                        
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
    
    def process_report_data(self, data: List[Dict[str, Any]]) -> bool:
        try:
            for report in data:
                business_id = self.business_processor.get_business_id(report["business"]["name"])
                if not business_id:
                   business_type_id = self.business_processor.get_business_type(report["business"]["type"], report["business"]["category"])
                   biz_data = report["business"]
                   insert_biz_data = {k: v for k, v in biz_data.items() if k != 'category'}
                   insert_biz_data["business_type_id"] = business_type_id
                   business_id = self.business_processor.insert_business_data(insert_biz_data)
                
                # Create report data dictionary
                report_data = {
                    "reported_at": report["reported_at"],
                    "reported_to": report["reported_to"],
                    "number": report["number"],
                    "content_body": report["content_body"],
                    "business_id": business_id
                }
                report_id = self.report_processor.insert_report_data(report_data)
                
                # 처분 날짜가 있으면 
                if report["disposition"]["received_at"]:
                    disposition_data = report["disposition"]
                    disposition_data["report_id"] = report_id
                    self.report_processor.insert_report_disposition(disposition_data)
                    
        except Exception as e:
            print(f"Error processing report data: {e}")
            return False
        return True
    
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
                    charge_id = self.common_processor.get_charge_id(charge, None)
                    self.accusation_processor.insert_accusation_charge(accusation_id, charge_id)
            return True
        except Exception as e:
            print(f"Error processing accusation data: {e}")
            return False
    
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
    