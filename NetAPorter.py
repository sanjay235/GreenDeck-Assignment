# Importing required libraries.
import pandas as pd
import gc

# The constants used for checking the filter operands.
OPERAND_1_DISCOUNT = 'discount'
OPERAND_1_BRAND_NAME = 'brand.name'
OPERAND_1_COMPETITION = 'competition'
OPERAND_1_DISCOUNT_DIFF = 'discount_diff'

class NetAPorter:
    """
    The class implements the various methods for getting NetAPorter product IDs given some criteria.

    Attributes:
        path (string): The path to the JSON file.
        data (pandas.DataFrame): The dataframe format of the JSON file read.
        readData (boolean): The variable is used as flag to read the initial data.

    Parameters:
        path (string): The path to the JSON file.   
    """

    def __init__(self, path='dumps/netaporter_gb.json'):
        
        self.path = path
        self.data = None
        self.readData = True

    def readAndPreprocess(self):
        """
        The method implements the reading of the JSON file and does the required preprocessing of the same.
        """

        print('===============Started reading JSON===============')
        self.data = pd.read_json(self.path, lines=True, orient='columns')
        print('==============Completed reading JSON==============')
        
        # Replacing the JSON objects with actual IDs.
        self.data['_id'] = self.data['_id'].transform(lambda value: value.get('$oid'))
        
        # Calculating the discount percent.
        self.data['discount_percent'] = self.data['price'].transform(lambda value: 100 * abs(value.get('regular_price').get('value') - value.get('offer_price').get('value')) / value.get('regular_price').get('value'))

        # Getting the price of each product for easy computation later.
        self.data['nap_price'] = self.data['price'].transform(lambda value: value.get('basket_price').get('value', 0.))

        # Getting the price of similar product from competitior for easy computation later.
        self.data['competitor_price'] = self.data['similar_products'].apply(self.similarProductPrice)

        # Calculating the discount difference between the NetAPorter and competitor price.
        # [[ NAP_price = Competitor_price + x/100 * Competitor_price ]] 
        # i.e [[ x = (N-C)/C * 100 ]]
        self.data['discount_diff'] = (self.data['nap_price'] - self.data['competitor_price']) / self.data['competitor_price']

        self.data['discount_diff'] = self.data['discount_diff'].transform(lambda value: value * 100. if value > 0. else 0.)
        
        required_columns = ['_id', 'discount_percent', 'brand', 'similar_products', 'price', 'nap_price', 'competitor_price', 'discount_diff']
        
        # Storing only required columns data for filtering later.
        self.data = self.data[ required_columns ].copy()

    def readQuery(self, json_dict):
        """
        The method implements the routing of query based on the requirement in JSON request.

        Parameters:
            json_dict (dictionary): The JSON request in the form of dictionary object.

        Returns:
            dictionary: The filtered data from the particular query method. 
        """
        
        # Initial check for reading and preprocessing JSON data.
        if self.readData:
            self.readAndPreprocess()
            self.readData = not self.readData
        
        # Getting the query type value.
        query = json_dict.get("query_type").strip()

        # Choosing the appropriate method based on query type value.
        if query == "discounted_products_list":
            return self.execQueryType1(json_dict.get("filters", []))
        
        elif query == "discounted_products_count|avg_discount":
            return self.execQueryType2(json_dict.get("filters", []))
        
        elif query == "expensive_list":
            return self.execQueryType3(json_dict.get("filters", []))
        
        elif query == "competition_discount_diff_list":
            return self.execQueryType4(json_dict.get("filters", []))

    def execQueryType1(self, filters):
        """
        The method implements the Query to get NAP products where discount is greater than "n%".

        Parameters:
            filters (dictionary): The actual filtering conditions for the data.

        Returns:
            dictionary: The filtered data from the particular query method. 
        """

        if not len(filters):
            return { 'discounted_products_list':[] }

        partial_data = self.data[['_id', 'discount_percent', 'brand', 'similar_products']].copy()

        #=============Re-Usable Filter Function=============#
        partial_data = self.filterData(filters, partial_data)
        #=============Re-Usable Filter Function=============#
        
        return { "discounted_products_list": partial_data['_id'].to_list() }

    def execQueryType2(self, filters):
        """
        The method implements the Query to get Count of NAP products 
        from a particular brand and its average discount.

        Parameters:
            filters (dictionary): The actual filtering conditions for the data.

        Returns:
            dictionary: The filtered data from the particular query method. 
        """

        if not len(filters):
            return { "discounted_products_count": 0, "avg_dicount": 0. }
        
        partial_data = self.data[['_id', 'discount_percent', 'brand', 'similar_products']].copy()

        #=============Re-Usable Filter Function=============#
        partial_data = self.filterData(filters, partial_data)
        #=============Re-Usable Filter Function=============#
        
        return { "discounted_products_count": partial_data.shape[0],\
        "avg_dicount": round(partial_data['discount_percent'].mean(), 2) if partial_data.shape[0] else 0. }

    def execQueryType3(self, filters):
        """
        The method implements the Query to get NAP products where they are selling at a price 
        higher than any of the competition.

        Parameters:
            filters (dictionary): The actual filtering conditions for the data.

        Returns:
            dictionary: The filtered data from the particular query method. 
        """

        partial_data = self.data[['_id', 'price', 'similar_products', 'brand', 'nap_price', 'competitor_price']].copy()

        #=============Re-Usable Filter Function=============#
        partial_data = self.filterData(filters, partial_data)
        #=============Re-Usable Filter Function=============#

        return { "expensive_list": partial_data['_id'][partial_data['nap_price'] > partial_data['competitor_price']].to_list() }

    def execQueryType4(self, filters):
        """
        The method implements the Query to get NAP products where they are selling at a price n% 
        higher than a competitor "X".

        Parameters:
            filters (dictionary): The actual filtering conditions for the data.

        Returns:
            dictionary: The filtered data from the particular query method. 
        """

        partial_data = self.data[['_id', 'price', 'similar_products', 'nap_price', 'competitor_price', 'discount_diff']].copy()
        
        #=============Re-Usable Filter Function=============#
        partial_data = self.filterData(filters, partial_data)
        #=============Re-Usable Filter Function=============#
            
        return { "competition_discount_diff_list": partial_data['_id'].to_list() }
    
    def filterData(self, filters, partial_data):
        """
        The method implements the filtering of the data as per given conditions.

        Parameters:
            filters (dictionary): The actual filtering conditions for the data.
            partial_data (pandas.DataFrame): The partial data that is used as a form of cache for filtering.

        Returns:
            pandas.Dataframe: The filtered data based on the conditions specified in the filters dictionary. 
        """

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
        """
        The method is used to get the similar product price from the competitor.

        Parameters:
            value (dictionary): The meta data related to the different competitiors.

        Returns:
            float: The price of similar product from the competitior.
        """
        
        for val in value.get('website_results').values():
            
            if len(val.get('knn_items')):
                
                return val.get('knn_items')[0].get('_source').get('price').get('basket_price').get('value', 0.)
                
        return 0.0

    def isCompetitior(self, value, competitior_id):
        """
        The method is used to check if a competitior exists with given competitor data.

        Parameters:
            value (dictionary): The meta data related to the different competitiors.
            competitior_id (string): The competitor ID to be checked for existence.

        Returns:
            boolean: The truth value for the competitior's presence.
        """

        for key, val in value.get('website_results').items():
            
            if key.strip() == competitior_id.strip() and len(val.get('knn_items')):
                
                return True
                
        return False
