import json
from datetime import datetime
from utils.response_codes import RESPONSE_CODES ,VALIDATION_MESSAGES ,DATABASE_MESSAGES ,VALIDIC_PAYLOAD_MESSAGES ,DATASTREAM_MESSAGES


# Function to create Standardized response

def std_response(code,custom_message=None):
    message = custom_message if custom_message else RESPONSE_CODES.get(code,"Unknown Error")
    #Build a standard response structure
    response = {
        "code":code,
        "message":message,
    }

    # Convert response to JSON
    return json.dumps(response)

# Example utility function for logging and error handling
def std_error_response(code, error_key, messages_dict):
    message = messages_dict.get(error_key, "Unknown reason")
    return std_response(code,custom_message=message)