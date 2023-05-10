from time import perf_counter
from base64 import b64decode
from aws_lambda_powertools import Logger
from dsl_utils.aws_wrappers.s3 import S3Bucket
from hl7_objects import Hl7Record, MasterFileJson, StateDoh, RepeatedHl7MessageError
from doc_db_mapper import ApiRequest
from hl7_message_utils import create_message
from state_level_validation_funcs import hl7_test_file_name
from config import DEBUG_MODE

LOGGER = Logger(service="dsl_hl7_kafka_order", level="DEBUG" if DEBUG_MODE else "INFO")


def set_logger_keys():
    LOGGER.append_keys(mrn="")
    LOGGER.append_keys(encounter_id="")
    LOGGER.append_keys(org_id="")
    LOGGER.append_keys(doh="")
    LOGGER.append_keys(assay="")
    LOGGER.append_keys(result_date="")
    LOGGER.append_keys(order_id="")


set_logger_keys()


def file_extension(file_format: str) -> str:
    """Function to determine the HL7 message extension

    Args:
        file_format (str)

    Raises:
        TypeError: If the extension is not found.

    Returns:
        str
    """
    if file_format == "TXT":
        return ".txt"
    elif file_format == "hl7":
        return ".hl7"
    else:
        raise TypeError("File extension not valid")


def save_message_in_s3(
    bucket_obj,
    hl7_message: str,
    file_location: str,
    file_name: str,
    file_format: str,
) -> None:
    """Function to store HL7 messages in S3.

    Args:
        bucket_obj (decryption_tools.DecryptData)
        hl7_message (str)
        file_location (str)
        file_name (str)
        file_format (str)
    """
    key_str = f"{file_location}/{file_name}"

    bucket_obj.put(
        Body=hl7_message,
        Key=f"{key_str}{file_extension(file_format = file_format)}",
    )


def main(
    hl7_message: dict,
    bucket_obj: S3Bucket,
):
    LOGGER.info("Grabbing master JSON.")
    try:
        message = Hl7Record(
            record=hl7_message, logger=LOGGER, MasterFileJson=MasterFileJson()
        )
    except RepeatedHl7MessageError as e:
        LOGGER.warning("HL7 message error. Error: " + str(e))
        return

    if not message.is_message_required():
        LOGGER.warning("Test result does not require message. Quitting.")
        return

    if not message.dohs:
        LOGGER.warning("Failed to find orgID " + message.facility["org_id"])
        return

    LOGGER.info("Grabbing state JSON.")
    state_dohs = StateDoh(dohs=message.dohs)

    LOGGER.info("Processing DOHs message")

    for doh in message.dohs:

        message.add_doh(input=doh)
        LOGGER.append_keys(doh=[doh.title()])
        state_level_message_check = message.state_config_validation(state=doh)

        if state_level_message_check:

            LOGGER.warning(state_level_message_check)
            continue

        doh_json = state_dohs.doh_data[doh]

        LOGGER.info("Constructing HL7 message.")
        hl7_message = create_message(
            data=message, doh_json=doh_json, master_file_obj=message.MasterFileJson
        )
        LOGGER.debug(hl7_message)

        LOGGER.info("Dropping file.")
        file_format = state_dohs.file_formats[doh]
        save_message_in_s3(
            bucket_obj=bucket_obj,
            hl7_message=hl7_message,
            file_location=state_dohs.file_locations[doh],
            file_name=hl7_test_file_name(state=doh, order_id=message.order["id"]),
            file_format=file_format,
        )

        LOGGER.info("HL7 message generation complete.")
        LOGGER.info("All steps run successfully.")

    LOGGER.append_keys(doh=[doh.title() for doh in message.dohs])


def process(payload, api_call, bucket_obj):
    """Kafka AWS Lambda Sink Connector Payload"""
    _id = b64decode(payload["payload"]["key"]).decode("utf-8")
    LOGGER.append_keys(encounter_id=_id)
    api_request = ApiRequest(encounter_id=_id, NomiApiCall=api_call)
    order_payload = api_request.get_order_data(_id)
    for order in order_payload:
        LOGGER.append_keys(order_id=order["order"]["id"])
        if order is None:
            LOGGER.warning("Lambda Finished Executing without generating message.")
            return
        start = perf_counter()
        LOGGER.debug(order)
        LOGGER.info(f"Total time calling tiger api: {perf_counter()-start}")
        main(
            hl7_message=order,
            bucket_obj=bucket_obj,
        )
        LOGGER.info(f"Total time processing hl7 message: {perf_counter()-start}")
