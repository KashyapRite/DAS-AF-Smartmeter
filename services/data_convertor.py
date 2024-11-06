import uuid
import hashlib
import datetime
import logging

class PayloadConversionService:
    def convert_data(self,payload):
        """
        Converts the Smartmeter payload based on the reading_type.
            Dispatches to respective methods based on the reading_type.
            
            :param payload: dict - The validated Smartmeter payload
            :return: dict - The Validic formatted payload
        """
        #Extract reading type and dispatch accordingly
        try:
            reading_type = payload.get("reading_type")

            if reading_type == "blood_pressure":
                return self._convert_blood_pressure_payload(payload)
            elif reading_type == "blood_glucose":
                return self._convert_blood_glucose_payload(payload)
            elif reading_type == "weight":
                return self._convert_body_weight_payload(payload)
            elif reading_type == "pulse_ox":
                return self._convert_pulse_ox_payload(payload)
            else:
                return None
        except:
            return None
    def _generate_common_fields(self,payload):
        """
            Generates common fields like data_id, checksum, and timestamps.
            
            :param payload: dict - The validated Smartmeter payload
            :return: dict - Common fields used across all reading types
        """
        try:
            #Generate UUID for data_id
            data_id = str(uuid.uuid4())

            #Generate checksum
            checksum_str = f'{payload["reading_id"]}{payload["imei"]}'
            checksum = hashlib.md5(checksum_str.encode()).hexdigest()

            # Convert date formats
            start_date = payload["date_recorded"]
            utc_offset = int(payload.get("time_zone_offset",0)*3600)  # Offset into seconds

            common_fields = {
                "data_id":data_id,
                "checksum":checksum,
                "log_id":payload.get("reading_id"),
                "source_type":"Smartmeter",
                "start_time":start_date,
                "end_time":start_date,
                "utc_offset":utc_offset
            }

            return common_fields
        except:
            return None

    def _convert_blood_pressure_payload(self,payload):
        """Converts blood pressure payload to Validic format."""
        common_fields = self._generate_common_fields(payload)

        try:
            validic_payload = {
                **common_fields,
                "log_data":{
                    "metrics":[
                        {"type":"systolic","value":payload["systolic_mmhg"],"unit":"mmHg","origin":"device"},
                        {"type":"diastolic","value":payload["diastolic_mmhg"],"unit":"mmHg","origin":"device"},
                        {"type":"pulse","value":payload["pulse_bpm"],"unit":"bpm","origin":"device"}
                    ],
                    "offset_origin":"source",
                    "source":{
                        "device":{
                        "imei": payload["imei"],
                        "model":payload["device_model"],
                        "manufacturer":"Smartmeter"
                        },
                        "type":payload["reading_type"]
                    },
                    "tags": [],
                    "type": "measurement",
                    "user":{
                        "organization_id":"5ff638fcfbbe58000165a00e",
                        "uid": "446d68d22cef4a1087210a8ff669ebd7-p10e100c",
                        "user_id": "6655ee548533ed0011b17f99",
                    },
                    "user_notes":[],
                }
            }
            logging.info(f"Converted blood pressure payload")
            return validic_payload
        except:
            return None
    
    def _convert_blood_glucose_payload(self,payload):
        """Convert smart meter blood glucose to validic format"""
        common_fields = self._generate_common_fields(payload)

        try:
            validic_payload = {
                **common_fields,
                "log_data":{
                    "metrics":[
                        {"type":"blood_glucose","value":payload["blood_glucose_mgdl"],"unit":"mg/dL","origin":"device"},
                        {"type":"before_meal","value":payload["before_meal"],"unit":"boolean","origin":"device"}
                    ],
                    "offset_origin": int(payload.get("time_zone_offset",0)*3600),
                    "source":{
                        "device":{
                        "imei": payload["imei"],
                        "model":payload["device_model"],
                        "manufacturer":"Smartmeter"
                        },
                        "type":payload["reading_type"],
                        "tags": [],
                    "type": "measurement",
                    "user":{
                        "organization_id":"5ff638fcfbbe58000165a00e",
                        "uid": "446d68d22cef4a1087210a8ff669ebd7-p10e100c",
                        "user_id": "6655ee548533ed0011b17f99",
                    },
                    "user_notes":[],
                    }
                }            
            }
            logging.info(f"Converted blood glucose payload")
            return validic_payload
        except:
            return None
    
    def _convert_body_weight_payload(self,payload):
        """Converts body weight payload to Validic format."""
        common_fields = self._generate_common_fields(payload)
        try:
            validic_payload = {
                **common_fields,
                "log_data":{
                    "metrics":[
                        {"type":"body_weight","value":payload["weight_kg"],"unit":"kg","origin":"device"},
                        {"type":"bmi","value":"calculate","unit":"kg/m2","origin":"device"}
                    ],
                    "offset_origin": int(payload.get("time_zone_offset",0)*3600),
                    "source":{
                        "device":{
                        "imei": payload["imei"],
                        "model":payload["device_model"],
                        "manufacturer":"Smartmeter"
                        },
                        "type":payload["reading_type"],
                        "tags": [],
                    "type": "measurement",
                    "user":{
                        "organization_id":"5ff638fcfbbe58000165a00e",
                        "uid": "446d68d22cef4a1087210a8ff669ebd7-p10e100c",
                        "user_id": "6655ee548533ed0011b17f99",
                    },
                    "user_notes":[],
                    }
                }
            }
            logging.info(f"Converted body weight payload")
            return validic_payload
        except:
            return None
    def _convert_pulse_ox_payload(self,payload):
        """Converts Pulse OX payload to Validic format."""
        common_fields = self._generate_common_fields(payload)
        try:
            validic_payload = {
                **common_fields,
                "log_data":{
                    "metrics":[
                        {"type":"spo2","value":payload["spo2"],"unit":"percent","origin":"device"},
                        {"type":"pulse","value":payload["pulse_bpm"],"unit":"bpm","origin":"device"}
                    ],
                    "offset_origin":"source",
                    "source":{
                        "device":{
                        "imei": payload["imei"],
                        "model":payload["device_model"],
                        "manufacturer":"Smartmeter"
                        },
                        "type":payload["reading_type"]
                    },
                    "tags": [],
                    "type": "measurement",
                    "user":{
                        "organization_id":"5ff638fcfbbe58000165a00e",
                        "uid": "446d68d22cef4a1087210a8ff669ebd7-p10e100c",
                        "user_id": "6655ee548533ed0011b17f99",
                    },
                    "user_notes":[],
                }
            }
            logging.info(f"Converted Pulse OX payload")
            return validic_payload
        except:
            return None

"""
NOTE:- In this service , there are few more logical things missing . Few fields are hard-coded which need to defined , calculate or get from
marketplace , generate for each record. 
"""        

