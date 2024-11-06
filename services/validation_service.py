import datetime
from services.db_service import DataBaseService
from utils.exceptions import DatabaseError, ValidationError
from utils.logger import setup_logging
import logging

# Call the setup_logging function to initialize logging
setup_logging()

#Initiatize the database service
db_service = DataBaseService()
db_service.connect_to_db()
def validate_payload(payload):
  
        # Further validation: Check if reading_type is 'blood_pressure'
        if payload.get("reading_type")=="blood_pressure":
            systolic = payload.get("systolic_mmhg")
            diastolic = payload.get("diastolic_mmhg")
            bpm = payload.get("pulse_bpm")
            device_id_BP = payload.get("device_id")
            time_zone_offset_BP = payload.get("time_zone_offset")
            reading_id_BP = payload.get("reading_id")

            if systolic is None or diastolic is None or bpm is None or device_id_BP is None or time_zone_offset_BP is None or reading_id_BP is None: 
                reason = "Missing blood pressure fields in payload"
                db_service.log_discarded_payload(payload, reason)
                logging.error(reason)
                return False

        # Further validation: Check if reading_type is 'blood_glucose'
        elif payload.get("reading_type")=="blood_glucose":
            blood_glucose_mgdl = payload.get("blood_glucose_mgdl")
            blood_glucose_mmol = payload.get("blood_glucose_mmol")
            before_meal = payload.get("before_meal")
            device_id_GLUC = payload.get("device_id")
            time_zone_offset_GLUC = payload.get("time_zone_offset")
            reading_id_GLUC = payload.get("reading_id")

            if blood_glucose_mgdl is None or blood_glucose_mmol is None or before_meal is None or device_id_GLUC is None or time_zone_offset_GLUC is None or reading_id_GLUC is None:
                reason = "Missing blood Glucose fields in payload"
                db_service.log_discarded_payload(payload, reason)
                logging.error(reason)
                return False
            
        elif payload.get("reading_type")=="weight":
            weight_kg = payload.get("weight_kg")
            weight_lbs = payload.get("weight_lbs")
            device_id_wt = payload.get("device_id")
            time_zone_offset_wt = payload.get("time_zone_offset")
            reading_id_wt = payload.get("reading_id")

            if weight_kg is None or weight_lbs is None or device_id_wt is None or time_zone_offset_wt is None or reading_id_wt is None:
                reason = "Missing body Weight fields in payload"
                db_service.log_discarded_payload(payload, reason)
                logging.error(reason)
                return False

        elif payload.get("reading_type")=="pulse_ox":
            spo2 = payload.get("spo2")
            pulse_bpm = payload.get("pulse_bpm")
            device_id_ox = payload.get("device_id")
            time_zone_offset_OX = payload.get("time_zone_offset")
            reading_id_ox = payload.get("reading_id")


            if spo2 is None or pulse_bpm is None or device_id_ox is None or time_zone_offset_OX is None or reading_id_ox is None :
                reason = "Missing PulseOX fields in payload"
                db_service.log_discarded_payload(payload, reason)
                logging.error(reason)
                return False
            
        else:
            reason = "Missing reading type field in the payload"
            db_service.log_discarded_payload(payload,reason)
            logging.error(reason)
            return False

        # If all validations pass
        logging.info("Payload is valid for processing.")
        return True



# # Example payload to validate
# smartmeter_payload = {
#                 "reading_id": 500000000000372,
#                 "device_id": "9999703",
#                 "device_model": "SM5000-IB",
#                 "date_recorded": "2020-01-01T07:29:00",
#                 "date_received": "2020-01-01T12:29:02",
#                 "reading_type": "blood_pressure",
#                 "systolic_mmhg": 128,
#                 "diastolic_mmhg": 91,
#                 "pulse_bpm": 71,
#                 "irregular": False,
#                 "time_zone_offset": -5.0,
#                 "short_code": None,
#                 "qa": None,
#                 "imei": "000000005060990",
#                 "device_user": 1
# }

# # Run validation
# if validate_payload(smartmeter_payload):
#     logging.info("Payload accepted for further processing.")
# else:
#     logging.info("Payload discarded.")    
        
# # Close the database connection
# db_service.close_db_connection()




