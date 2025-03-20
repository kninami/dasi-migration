import supabase
import os 
import dotenv

class DbProcessor:
    def __init__(self):
        dotenv.load_dotenv()
        
        self.SUPABASE_URL = os.getenv('SUPABASE_URL')
        self.SUPABASE_KEY = os.getenv('SUPABASE_KEY')        
        self.supabase = supabase.create_client(self.SUPABASE_URL, self.SUPABASE_KEY)
    
    def process_data(self, data_array):
        for data in data_array:
            business_data = data['business']
            biz_insert = {k: v for k, v in business_data.items() if k != 'category'}
            biz_insert['business_type_id'] = self.get_business_type(business_data['type'], business_data['category'])
            print(biz_insert)
            
            # business_id = self.insert_business_data(business_data)
            # for person in data['people']:
            #     accused_people = {
            #         'business_id': business_id,
            #         'name': person['name'],
            #         'role': person['role']
            #     }
            #     self.insert_accusation_data(accused_people)
    
    def get_business_type(self, type, category):
        response = self.supabase.table('business_types').select('id').eq('name', category).execute()
        if len(response.data) == 0:  # 데이터가 없는 경우
            response = self.supabase.table('business_types').insert({'type': type, 'name': category}).execute()
        return response.data[0]['id']
    
    def insert_accusation_data(self, data):
        accusation_id = self.supabase.table('accusation_accused_persons').insert(data).execute()
        return accusation_id
    
    def insert_business_data(self, data):
        business_id = self.supabase.table('business').insert(data).execute()
        return business_id

# 사용 예시
if __name__ == "__main__":
    processor = DbProcessor()
    # processor.process_data(your_data_array)