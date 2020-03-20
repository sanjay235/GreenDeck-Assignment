import pandas as pd

OPERAND_1_DISCOUNT = 'discount'
OPERAND_1_BRAND_NAME = 'brand.name'
OPERAND_1_COMPETITION = 'competition'
OPERAND_1_DISCOUNT_DIFF = 'discount_diff'

class NetAPorter:

    def __init__(self, path='dumps/netaporter_gb.json'):
        
        self.path = path
        self.data = None
        self.readData = True

    def readAndPreprocess(self):
        
        print('===============Started reading JSON===============')
        total_data = pd.read_json(self.path, lines=True, orient='columns')
        print('==============Completed reading JSON==============')
        
        total_data['_id'] = total_data['_id'].transform(lambda value: value.get('$oid'))
        
        total_data['discount_percent'] = total_data['price'].transform(lambda value: 100 * abs(value.get('regular_price').get('value') - value.get('offer_price').get('value')) / value.get('regular_price').get('value'))

        total_data['nap_price'] = total_data['price'].transform(lambda value: value.get('basket_price').get('value', 0.))

        total_data['competitor_price'] = total_data['similar_products'].apply(self.similarProductPrice)

        # [[ NAP_price = Competitor_price + x/100 * Competitor_price ]] ==> [[ x = (N-C)/C * 100 ]]
        total_data['discount_diff'] = (total_data['nap_price'] - total_data['competitor_price']) / total_data['competitor_price']

        total_data['discount_diff'] = total_data['discount_diff'].transform(lambda value: value * 100. if value > 0. else 0.)
        
        required_columns = ['_id', 'discount_percent', 'brand', 'similar_products', 'price', 'nap_price', 'competitor_price', 'discount_diff']
        
        self.data = total_data[ required_columns ].copy()

        total_data, required_columns = None, None

    def readQuery(self, json_dict):

        if self.readData:
            self.readAndPreprocess()
            self.readData = not self.readData

        query = json_dict.get("query_type").strip()

        if query == "discounted_products_list":
            return self.execQueryType1(json_dict.get("filters", []))
        
        elif query == "discounted_products_count|avg_discount":
            return self.execQueryType2(json_dict.get("filters", []))
        
        elif query == "expensive_list":
            return self.execQueryType3(json_dict.get("filters", []))
        
        elif query == "competition_discount_diff_list":
            return self.execQueryType4(json_dict.get("filters", []))

    def execQueryType1(self, filters):
        
        if not len(filters):
            return { 'discounted_products_list':[] }

        partial_data = self.data[['_id', 'discount_percent', 'brand', 'similar_products']].copy()

        #=============Re-Usable Filter Function=============#
        partial_data = self.filterData(filters, partial_data)
        #=============Re-Usable Filter Function=============#
        
        return { "discounted_products_list": partial_data['_id'].shape[0] }

    def execQueryType2(self, filters):
        
        if not len(filters):
            return { "discounted_products_count": 0, "avg_dicount": 0. }
        
        partial_data = self.data[['_id', 'discount_percent', 'brand', 'similar_products']].copy()

        #=============Re-Usable Filter Function=============#
        partial_data = self.filterData(filters, partial_data)
        #=============Re-Usable Filter Function=============#
        
        return { "discounted_products_count": partial_data.shape[0],\
        "avg_dicount": round(partial_data['discount_percent'].mean(), 2) if partial_data.shape[0] else 0. }

    def execQueryType3(self, filters):

        partial_data = self.data[['_id', 'price', 'similar_products', 'brand', 'nap_price', 'competitor_price']].copy()

        #=============Re-Usable Filter Function=============#
        partial_data = self.filterData(filters, partial_data)
        #=============Re-Usable Filter Function=============#

        return { "expensive_list": partial_data['_id'][partial_data['nap_price'] > partial_data['competitor_price']].shape[0] }

    def execQueryType4(self, filters):
        
        partial_data = self.data[['_id', 'price', 'similar_products', 'nap_price', 'competitor_price', 'discount_diff']].copy()
        
        #=============Re-Usable Filter Function=============#
        partial_data = self.filterData(filters, partial_data)
        #=============Re-Usable Filter Function=============#
            
        return { "competition_discount_diff_list": partial_data['_id'].shape[0] }
    
    def filterData(self, filters, partial_data):

        for filt in filters:
            
            if filt.get('operator').strip() == '==':
                
                if filt.get('operand1').strip() == OPERAND_1_BRAND_NAME:
                    
                    partial_data = partial_data[ partial_data['brand'].transform(lambda value: value.get('name') == filt.get('operand2').strip()) ]
                
                elif filt.get('operand1').strip() == OPERAND_1_COMPETITION:
                    
                    partial_data = partial_data[ partial_data['similar_products'].apply(lambda value: self.isCompetitior(value, filt.get('operand2').strip())) ]
            
            elif filt.get('operator').strip() == '<' and (filt.get('operand1').strip() == OPERAND_1_DISCOUNT or filt.get('operand1').strip() == OPERAND_1_DISCOUNT_DIFF):
                
                column = 'discount_percent' if filt.get('operand1').strip() == OPERAND_1_DISCOUNT else 'discount_diff'
                
                partial_data = partial_data[ partial_data[ column ] < float(filt.get('operand2')) ]
            
            elif filt.get('operator').strip() == '>' and (filt.get('operand1').strip() == OPERAND_1_DISCOUNT or filt.get('operand1').strip() == OPERAND_1_DISCOUNT_DIFF):
                
                column = 'discount_percent' if filt.get('operand1').strip() == OPERAND_1_DISCOUNT else 'discount_diff'

                partial_data = partial_data[ partial_data[ column ] > float(filt.get('operand2')) ]

        return partial_data

    def similarProductPrice(self, value):
        
        for key, val in value.get('website_results').items():
            
            if len(val.get('knn_items')):
                
                return val.get('knn_items')[0].get('_source').get('price').get('basket_price').get('value', 0.)
                
        return 0.0

    def isCompetitior(self, value, competitior_id):
        
        for key, val in value.get('website_results').items():
            
            if key.strip() == competitior_id.strip() and len(val.get('knn_items')):
                
                return True
                
        return False
