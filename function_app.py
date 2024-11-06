# Import external Modules
import logging
import json
import os
import sys

# Add system path

sys.path.append("C:/Kashyap/Github/DAS-AF-Smartmeter-V2")

# Import internal Modules
from services.db_service import DataBaseService
from services.validation_service import validate_payload
from utils.logger import setup_logging
from services.data_convertor import PayloadConversionService
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from utils.response_codes import VALIDATION_MESSAGES , RESPONSE_CODES , DATABASE_MESSAGES , DATASTREAM_MESSAGES , VALIDIC_PAYLOAD_MESSAGES ,FUNCTION_ERROR
from services.response_helper import std_response, std_error_response
import azure.functions as func

# Loading local.settings.json 
with open("local.settings.json",'r') as f:
    settings = json.load(f)

# Setup logging
setup_logging()

# Initialize the database service
db_service = DataBaseService()
db_service.connect_to_db()

# Initialize the payload conversion service.
payload_converter = PayloadConversionService()

# Add Service Bus connection string
SERVICE_BUS_CONNECTION_STRING = settings.get("Values",{}).get("AZURE_SERVICE_BUS_CONNECTION_STRING")
queue_name = "datastreamqueue"

# Initialize the Service Bus client
servicebus_client = ServiceBusClient.from_connection_string(conn_str=SERVICE_BUS_CONNECTION_STRING)


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name(name="smartmeterWebhook")
@app.route(route="smartmeterWebhook", methods=["POST"])
def smartmeterWebhook(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Smartmeter Webhook has received a request in V2 model.')

    try:
        #Step1:- Get smartmeter payload and and log it
        payload = req.get_json()
        logging.info("Received Json Payload")
        print("Deployment checking")

        #Step2:- Validate IMEI
        imei = payload.get("imei")
        if not imei:
            reason = "IMEI is missing in the payload"
            db_service.log_discarded_payload(payload,reason) # Log discarded payload
            logging.warning(reason)
            return func.HttpResponse(std_error_response(RESPONSE_CODES["BAD_REQUEST"],"IMEI_MISSING",DATABASE_MESSAGES), status_code=400)
        
        device_data = db_service.fetch_data(payload)

        if not device_data:
            reason = "No device found with the given IMEI."
            logging.error(reason)
            return func.HttpResponse(std_error_response(RESPONSE_CODES["BAD_REQUEST"],"DEVICE_NOT_FOUND",DATABASE_MESSAGES), status_code=404)   
        
        # Step3: Validate payload fields
        if validate_payload(payload):
            # Step 4 :- Convert payload to Validic format.Converted Validic Payload
            validic_payload = payload_converter.convert_data(payload)
            logging.info('Converted Validic Payload.....')
            if validic_payload is None:
                logging.error("Error in payload conversion")
                return func.HttpResponse(std_error_response(RESPONSE_CODES["ERROR"],"INVALID_FORMAT",VALIDIC_PAYLOAD_MESSAGES), status_code=400)
        else:
            reason = "Payload validation failed."
            logging.error(reason)
            return func.HttpResponse(std_error_response(RESPONSE_CODES["VALIDATION_ERROR"],"MISSING_FIELDS",VALIDATION_MESSAGES), status_code=400)
        
        # Step 5: Send the Validic payload to the DataStream function.
        if validic_payload is not None:
            with servicebus_client.get_queue_sender(queue_name) as sender:
                # Create a new message with the payload
                message = ServiceBusMessage(json.dumps(validic_payload))
                sender.send_messages(message)  # Send the message to the queue.
                logging.info("Validic payload sent to Service Bus queue...")

            return func.HttpResponse(
                std_response(RESPONSE_CODES["SUCCESS"],f"{validic_payload},Validic Payload is send successfully to Service Bus queue."),
                mimetype="application/json",
                status_code=200
            )
        
        else:    
            logging.error("Error calling DataStream function:")
            return func.HttpResponse(std_error_response(RESPONSE_CODES["INTERNAL_SERVER_ERROR"],"CONNECTION_ERROR",DATASTREAM_MESSAGES), status_code=500)
        
    
    except Exception as e:
        logging.error(f"Unexpected Error : {str(e)}")    
        return func.HttpResponse(std_error_response(RESPONSE_CODES["INTERNAL_SERVER_ERROR"],"INTERNAL_SERVER_ERROR",FUNCTION_ERROR), status_code=500)
    


    # finally:
    #     db_service.close_db_connection()
    """
    NOTE:- This db connection close is closed directly when the one payload is received and processed , so need to 
    look after this case to close the connection properly.
    """