from time import perf_counter
from base64 import b64decode
from aws_lambda_powertools import Logger
from dsl_utils.nomi_apis.api_call import NomiApiCall
from dsl_utils.aws_wrappers.secrets_manager import AwsSecretManager
from dsl_utils.aws_wrappers.s3 import S3Bucket
import dsl_test_encounters as test
import dsl_vaccine_encounters as vax

from config import (
    DESTINATION_BUCKET,
    DOCDB_OAUTH_BASE_URL,
    DOC_DB_BASE_URL,
    SECRET_MANAGER_HL7_ARN,
    API_RETRY,
    DEBUG_MODE,
    APIS_TIMEOUT_TIME,
)

LOGGER = Logger(service="dsl_hl7_kafka_order", level="DEBUG" if DEBUG_MODE else "INFO")


def set_logger_keys():
    LOGGER.append_keys(mrn="")
    LOGGER.append_keys(order_id="")
    LOGGER.append_keys(org_id="")
    LOGGER.append_keys(doh="")
    LOGGER.append_keys(assay="")
    LOGGER.append_keys(result_date="")


set_logger_keys()


# {"type": "vaccine"/"test", "_id": "111", "payload": {}} - clarify with services which we'll be receiving _id or payload or both
# use the encounter_id to call order endpoint to get back all orders (search endpoint) (only for testing since vax doesn't do orders)
# update unit tests - QA

def lambda_handler(event, context):
    """Kafka AWS Lambda Sink Connector Payload
    [
        {
            "payload": {
                "topic": "NH-CARE-ENCOUNTER-COMPLETE",
                "partition": 1,
                "offset": 101,
                "key": "cmNtMTAwMjA2OW5r",
                "value": {},
                "timestamp": 1648023053240
            }
        }
    ]
    """
    LOGGER.info("HL7 Tiger Listener Lambda Triggered.")
    LOGGER.debug(event)

    try:
        start_lambda = perf_counter()

        LOGGER.info("Instantiating S3 bucket, DocDB and Secret Manager objects.")
        secrets_manager = AwsSecretManager(SECRET_MANAGER_HL7_ARN)
        token_tiger = secrets_manager.get_secret_token(secret_key="tiger_api_key")

        api_call = NomiApiCall(
            endpoint_url=DOC_DB_BASE_URL,
            oauth_token=f"Basic {token_tiger}",
            oauth_base_url=DOCDB_OAUTH_BASE_URL,
            max_attempts=API_RETRY,
            timeout_time=APIS_TIMEOUT_TIME,
        )

        bucket_obj = S3Bucket(DESTINATION_BUCKET)

        for kafka_record in event:
            try:

                LOGGER.info("Requesting data from Tiger.")

                _id = b64decode(kafka_record["payload"]["key"]).decode("utf-8")
                LOGGER.append_keys(_id=_id)
                LOGGER.info("Calling Tiger API.")
                test.process(kafka_record, api_call, bucket_obj)
                    # TODO will need to uncomment this logic later.
                    # if kafka_record["payload"]["type"] == "vaccine": 
                    #     vax.process(kafka_record, api_call, bucket_obj)
                    # else:
                    #     test.process(kafka_record, api_call, bucket_obj)

            except Exception as e:
                LOGGER.warning("HL7 message error. Error: " + str(e))

        LOGGER.info(f"Total lambda execution time: {perf_counter() - start_lambda}")
        LOGGER.info("Lambda Finished Executing.")
        return

    except Exception as e:
        LOGGER.append_keys(
            event_order_ids=[
                b64decode(message["payload"]["key"]).decode("utf-8")
                for message in event
            ]
        )
        LOGGER.critical("HL7 lambda error. Error: " + str(e))

        if DEBUG_MODE:
            return
        else:
            raise e


#                      ______
#                      |o  |   !
#    .--._____,        |:`_|---'-.
# .-='=='==-, ______.-.'_'.-----.|
# (O_o_o_o_o_O)       ''._.'     (O)
