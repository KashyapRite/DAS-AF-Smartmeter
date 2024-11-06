import pymongo
import os
import sys


# Add system path
sys.path.append("C:/Kashyap/Github/DAS-AF-Smartmeter-V2")

from utils.logger import setup_logging
from utils.exceptions import DatabaseError, ValidationError
import datetime
import logging
import json

# Loading the local.seting.json file
with open('local.settings.json', 'r') as f:
    settings = json.load(f)


# Configure logging
setup_logging()
# Add Service Bus connection string
DB_CONNECTION_STRING = settings.get("Values", {}).get("DB_CONNECTION_STRING")

class DataBaseService:
    def __init__(self):
        self.db_client = None
        self.db_name = 'das'
        self.collection_name = 'users'
        self.discarded_collection_name = 'logs'

    def connect_to_db(self):
        """Connect to the MongoDB instance"""
        try:
            self.db_client = pymongo.MongoClient(DB_CONNECTION_STRING)
            logging.info("Successfully connected to MongoDB.")
        except Exception as e:
            raise DatabaseError(f"Failed to connect to database:{str(e)}") 
        
    def fetch_data(self,payload):
        """Validate IMEI and fetch device data from MongoDB"""
        imei = payload.get("imei")        
        try:
            db = self.db_client[self.db_name]
            collection = db[self.collection_name]

            #Query to find a document where specific imei is present
            document = collection.find_one({
                    "sources":{
                        "$elemMatch":{"imei":imei},
                    }
                },
                {"_id": 0}
            )
            
            if not document:
                reason = f'No document found with IMEI:{imei}'
                self.log_discarded_payload(payload, reason)  # Log the discarded payload
                return None

            return document["sources"][0] #Return the projected source fields of the users.
            '''
            NOTE:-This return document returns the entire payload the device_data in __init__.py file which we do not
            want . So this logic have to deal further . It is noted.
            '''
        except Exception as e:
            raise DatabaseError(f"Failed to fetch device data:{str(e)}")  
        
    def log_discarded_payload(self,payload,reason):
        """Log discarded payloads in the MongoDB logs collection"""
        try:
            db = self.db_client[self.db_name]
            discard_collection = db[self.discarded_collection_name] # Ensure this collection is defined
            discard_log = {
                "payload": payload,
                "reason": reason,
                "timestamp": datetime.datetime.utcnow()
            }
            discard_collection.insert_one(discard_log)
            logging.info(f"Discarded payload logged")    
        except Exception as e:
            raise DatabaseError(f"Failed to log discarded payload: {str(e)}")    

        
    def close_db_connection(self):
        if self.db_client:
            self.db_client.close()
            logging.info("Database connection closed.")
    
# if __name__ == "__main__":
#     db_service = DataBaseService()
#     db_service.connect_to_db()
    
#     imei_to_check = "000000005060990"  # Replace with the actual IMEI you want to check
#     device_data = db_service.fetch_data(imei_to_check)

#     if device_data:
#         logging.info(f"Device data fetched: {device_data}")
#     else:
#         logging.warning("No device data found for the specified IMEI.")

#     db_service.close_db_connection()