from os import environ

DESTINATION_BUCKET = environ["DESTINATION_BUCKET"]

DOCDB_OAUTH_BASE_URL = environ["OAUTH_BASE_URL"]
DOC_DB_BASE_URL = environ["BASE_URL"]

SECRET_MANAGER_HL7_ARN = environ["SECRET_MANAGER_HL7_ARN"]

DEBUG_MODE = True
PROCESS_REPEATED_MESSAGES = False

API_RETRY = 5
API_TOKEN_TIME_LIMIT = 3600
APIS_TIMEOUT_TIME = 10

API_CALL_MAX_ATTEMPTS = 5
API_CALL_SLEEP = 10

STATES_LOCAL_TIME_ZONE_ADJUSTMENT_FROM_UTC = {"hawaii": -10}
STATES_WITHOUT_UTC_ADJUSTMENT = [
    "nebraska",
    "iowa",
    "florida",
    "kentucky",
    "colorado",
    "texas",
    *STATES_LOCAL_TIME_ZONE_ADJUSTMENT_FROM_UTC.keys(),
]
PREPEND_NOMI_STATES = [
    "maryland",
]
ADD_COUNTIES = [
    "maryland",
]

# ----------------- State Validation Rules -----------------ß
VALIDATION_MAPPERS = {
    "iowa": {"c19": ["stop_negative"]},
    "florida": {"monkeypox": ["stop_negative", "perform_facility_override"]},
}
# TODO change to enounter from order?
# ----------------- Optional/Required properties for message generation -----------------ß
HL7_OPTIONAL_ARGS = [
    ("order", "test_kit_id"),
    ("facility", "address", "address"),
    ("facility", "address", "city"),
    ("facility", "address", "state"),
    ("facility", "address", "postal_code"),
    ("patient", "personal", "first_name"),
    ("patient", "personal", "last_name"),
    ("patient", "personal", "gender"),
    ("patient", "address", "street_2"),
    ("patient", "address", "city"),
    ("patient", "address", "state"),
    ("patient", "address", "postal_code"),
    ("patient", "address", "county"),
]
# TODO change to enounter from order?
HL7_REQUIRED_ARGS = [
    ("order", "patient_id"),
    ("order", "sample_date"),
    ("order", "id"),
    ("order", "states", "RESULTED"),
    ("facility", "org_id"),
    ("facility", "name"),
    ("test_kit_types", "assay"),
    ("patient", "personal", "dob"),
    ("patient", "address", "street_1"),
]
# TODO change to enounter from order?
HL7_REQUIRED_ARGS_IN_LISTS = [  # check for dicts inside lists
    {("order", "results"): "result"}
]
