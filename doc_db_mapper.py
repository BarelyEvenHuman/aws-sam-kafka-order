from typing import Union
from logging import INFO, DEBUG
from aws_lambda_powertools import Logger
from dsl_utils.nomi_apis.tiger import TigerApi
from dsl_utils.decorators import function_retry_decorator
from dsl_utils.nomi_apis.api_call import NomiApiCall

from config import (
    DEBUG_MODE,
    API_CALL_MAX_ATTEMPTS,
    API_CALL_SLEEP,
)

LOGGER = Logger(service="doc_db_mapper", level=DEBUG if DEBUG_MODE else INFO)
LOGGER.append_keys(order_id="")


def join_address(input_dict: dict) -> str:
    """Function to join address_1 and address_2 if both keys are present.
    Both keys are deleted and a new address key is created.

    If only address_1 is present, then address_1 is deleted and address key is returned.

    Args:
        input_dict (dict)

    Returns:
        str
    """
    logic_test = ["street_1" in input_dict, "street_2" in input_dict]

    if all(logic_test):
        input_dict["address"] = input_dict["street_1"] + " " + input_dict["street_2"]

        return
    if any(logic_test):
        input_dict["address"] = input_dict["street_1"]

        return


class ApiRequest(TigerApi):
    def __init__(
        self,
        NomiApiCall: NomiApiCall,
        order_id = None,
        encounter_id = None,
    ):
        super().__init__(NomiApiCall)

        self.order_id = order_id
        self.encounter_id = encounter_id

    @function_retry_decorator(API_CALL_MAX_ATTEMPTS, LOGGER, API_CALL_SLEEP, False)
    def get_data_from_database(self, *args, **kargs) -> Union[dict, list]:
        """Method to send API request to the desired DocDB Collection.

        Raises:
            ValueError: if reject_multiple_responses argument is passed and response list len is higher than 1.

        Returns:
            Union[dict,list]
        """
        return super().get_data_from_database(*args, **kargs)

    def get_order_data(self, _id) -> list:
        """Method to request all the data related to an encounter.

        Returns:
            list
        """
        try:
            collection = "order"
            order_data = self.get_data_from_database(
                self.create_url("order_search", _id, self.order_id)
            )
            order_payload = []
            for order in order_data:
                LOGGER.append_keys(order_id=order["id"])
                if (
                    "RESULTED" not in order["states"]
                ):  # this means sample_value is not complete
                    raise KeyError(
                        "Order does not have a RESULTED state (Order is not completed)."
                    )

                collection = "procedure"
                procedure_data = self.get_data_from_database(
                    self.create_url("procedure", "", order["procedure_type_id"])
                )

                collection = "test_kit_type"
                test_kit_types_data = self.get_data_from_database(
                    self.create_url("test_kit_type", "", order["test_kit_type_id"])
                )

                collection = "facility"
                facility_data = self.get_data_from_database(
                    self.create_url("facility", "", order["test_location_id"])
                )

                if "address" in facility_data:
                    join_address(input_dict=facility_data["address"])

                collection = "encounter"
                encounter_data = self.get_data_from_database(
                    self.create_url("encounter", _id, self.encounter_id)
                )

                assert len(encounter_data) == 1, "Multiple encounters found"
                encounter_data = encounter_data[0]

                collection = "patient"
                patient_data = self.get_data_from_database(
                    self.create_url("patient", "", order["patient_id"])
                )

                payload = {
                    "order": order,
                    "procedure": procedure_data,
                    "test_kit_types": test_kit_types_data,
                    "facility": facility_data,
                    "encounter": encounter_data,
                    "patient": patient_data,
                }
                order_payload.append(payload)

        except Exception as e:
            raise Exception(
                f"Failed to retrieve API information at collection: {collection}, error: {str(e)}"
            )
        return order_payload

    def get_encounter_data(self, _id) -> list:
        """Method to request all the data related to an encounter.
        This will only be used for vaccines. Orders have different encounter
        method above.
        Returns:
            list
        """
        try:
            collection = "encounter"
            encounter_data = self.get_data_from_database(
                self.create_url("encounter", _id, self.encounter_id)
            )
        except Exception as e:
            raise Exception(
                f"Failed to retrieve API information at collection: {collection}, error: {str(e)}"
            )

        return encounter_data