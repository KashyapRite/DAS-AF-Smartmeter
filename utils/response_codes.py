import os

RESPONSE_CODES = {
	"SUCCESS": "0",
	"CREATED": "1",
	"ERROR": "2",
	"UNAUTHORIZED": "3",
	"BAD_REQUEST": "4",
	"VALIDATION_ERROR": "5",
	"CONNECTIVITY_ERROR": "6",
	"NOT_FOUND": "7",
	"INTERNAL_SERVER_ERROR": "8",
	"INVALID": "9"
}

# Validation service related messages and reasons
VALIDATION_MESSAGES = {
    "MISSING_FIELDS": "Validation error: Missing required field",
    "INVALID_DATA": "Validation error: Invalid data format",
    
}

# Database services related message and reasons
DATABASE_MESSAGES = {
    "DB_CONNECTION_FAILED": "Failed to connect to database",
    "DEVICE_NOT_FOUND": "No document found with IMEI",
    "FETCH_DEVICE_DATA_FAILED": "Failed to fetch device data",
    "IMEI_MISSING":"IMEI is missing in the payload"
}


# Messages related to specific processes like Validic payload or data streams
VALIDIC_PAYLOAD_MESSAGES = {
    "INVALID_FORMAT": "Payload format is invalid for Data Conversion.",
    "SUCCESS": "Validic payload formatted successfully."
}

DATASTREAM_MESSAGES = {
    "SUCCESS": "Data successfully sent to Data Stream.",
    "CONNECTION_ERROR": "Failed to connect to the Data Stream.",
    "PUSH_FAILED":"Data was not sent to Data Stream"
}

FUNCTION_ERROR = {
    "INTERNAL_SERVER_ERROR":"Failed in execution of the azure function."
}

