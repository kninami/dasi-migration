import supabase
import os 
import dotenv

class DbProcessor:
    def __init__(self):
        dotenv.load_dotenv()
        
        self.SUPABASE_URL = os.getenv('SUPABASE_URL')
        self.SUPABASE_KEY = os.getenv('SUPABASE_KEY')        
        self.supabase = supabase.create_client(self.SUPABASE_URL, self.SUPABASE_KEY)
    
    def insert_accusation_data(self, business_id, accused_at, office):
        # Timestamp 객체를 문자열로 변환
        if hasattr(accused_at, 'isoformat'):
            accused_at = accused_at.isoformat()
        
        response = self.supabase.table('accusations').insert({'business_id': business_id, 'accused_at': accused_at, 'office': office}).execute()
        return response.data[0]['id']
    
    def insert_accusations(self, business_id, accused_at, office):
        accusation_id = self.insert_accusation_data(business_id, accused_at, office)
        return accusation_id
    
    def insert_accused_person(self, accusation_id, name, role):
        self.supabase.table('accusation_accused_persons').insert({'accusation_id': accusation_id, 'name': name, 'role': role}).execute()
    
    def get_charge_id(self, charge):
        response = self.supabase.table('charges').select('id').eq('name', charge).is_('detail_name', None).execute()
        if len(response.data) == 0: 
            response = self.supabase.table('charges').insert({'name': charge}).execute()
        return response.data[0]['id']
    
    def insert_accusation_charge(self, accusation_id, charge_id):
        self.supabase.table('accusation_charges').insert({'accusation_id': accusation_id, 'charge_id': charge_id}).execute()
    
    def process_accusation_data(self, data):
        for accusation in data:
            accusation_id = self.insert_accusation_data(accusation["business_id"], accusation["accused_at"], accusation["office"])
            print(accusation_id)
            accusation_person = accusation["accused_person"]
            
            for person in accusation_person:
                self.insert_accused_person(accusation_id, person["name"], person["role"])
            
            charges = accusation["charge"]
            for charge in charges:
                charge_id = self.get_charge_id(charge)
                self.insert_accusation_charge(accusation_id, charge_id)            
        return True
    
    def process_data(self, data_array):
        accusation_array = []
        for data in data_array:
            business_data = data['business']
            accusation_data = data['accusations']
            biz_insert = {k: v for k, v in business_data.items() if k != 'category'}
            biz_insert['business_type_id'] = self.get_business_type(business_data['type'], business_data['category'])            
            try:
                business_id = self.insert_business_data(biz_insert)
                accusation_data["business_id"] = business_id
                accusation_array.append(accusation_data)
            except Exception as e:
                print(e)
        try:
            self.process_accusation_data(accusation_array)
        except Exception as e:
            print(e)
            
    def get_business_type(self, type, category):
        response = self.supabase.table('business_types').select('id').eq('name', category).execute()
        if len(response.data) == 0:  # 데이터가 없는 경우
            response = self.supabase.table('business_types').insert({'type': type, 'name': category}).execute()
        return response.data[0]['id']
    
    def insert_business_data(self, data):
        response = self.supabase.table('businesses').insert(data).execute()
        return response.data[0]['id']