from json import load, dumps
from abc import ABC
from uuid import uuid4
from datetime import datetime, timedelta
from dateutil import parser
from typing import Callable, Union
from flatten_dict import flatten

from us import STATES as USA_STATES
from us import states as us_states_object

from dsl_utils.utils import path_join
from dsl_utils.utils import clean_str
from dsl_utils.utils import rm_characters


from config import (
    VALIDATION_MAPPERS,
    STATES_WITHOUT_UTC_ADJUSTMENT,
    PREPEND_NOMI_STATES,
    ADD_COUNTIES,
    PROCESS_REPEATED_MESSAGES,
    STATES_LOCAL_TIME_ZONE_ADJUSTMENT_FROM_UTC,
    HL7_OPTIONAL_ARGS,
    HL7_REQUIRED_ARGS,
    HL7_REQUIRED_ARGS_IN_LISTS,
)

import state_level_validation_funcs

from aws_lambda_powertools import logging


class MasterFileJson:
    pass


class RepeatedHl7MessageError(Exception):
    pass


def time_zone_state_adjustment(doh: str) -> str:

    if doh in STATES_LOCAL_TIME_ZONE_ADJUSTMENT_FROM_UTC:

        number = STATES_LOCAL_TIME_ZONE_ADJUSTMENT_FROM_UTC[doh]
        sign = "-" if number <= 0 else "+"
        number = abs(number) * 100

        return f"{sign}{number:04n}"

    return "-0000" if doh not in STATES_WITHOUT_UTC_ADJUSTMENT else ""


def prepend_nomi_to_facility_name(doh: str) -> str:
    return "Nomi Health  - " if doh in PREPEND_NOMI_STATES else ""


def hl7StringRead(some_string: str) -> str:
    """Function to control the representation of missing fields in HL7 messages.

    Args:
        some_string (str)

    Returns:
        str
    """
    return some_string or "^"


def findStateAbbreviation(state_name: str) -> str:
    """Function to get the state abbreviation from any representation of state names.

    Args:
        state_name (str)

    Returns:
        str
    """
    state = us_states_object.lookup(state_name)

    return state.abbr if state else ""


def parse_datetime_isoformat(state: str, date_time: Union[str, datetime]) -> datetime:
    """Function to parse strings in date-string ISO-8601 format. Optional local time zone depends on config.py file.

    Args:
        state (Union[str,datetime])
        date_time (str)

    Returns:
        datetime
    """
    if isinstance(date_time, datetime):
        date_time_object = date_time
    else:
        date_time_object = parser.isoparse(date_time)

    return (
        date_time_object
        + timedelta(hours=STATES_LOCAL_TIME_ZONE_ADJUSTMENT_FROM_UTC[state])
        if state in STATES_LOCAL_TIME_ZONE_ADJUSTMENT_FROM_UTC
        else date_time_object
    )


class SharedMethods(ABC):
    """Abstract class to be use as database engine template."""

    def __init__(self, *args, **kwargs) -> None:
        """Constructor for Immutable database engine class."""

        private_components = {f"_{k}": v for k, v in kwargs.items()}
        self.__dict__.update(private_components)
        self.__dict__["_args"] = args
        self.__dict__["_STATES"] = [state.abbr for state in USA_STATES]
        self.__dict__["_doh"] = None

    def __getattr__(self, name):
        private_name = f"_{name}"
        try:
            return self.__dict__[private_name]
        except KeyError:
            raise AttributeError(f"{self!r} object has no attribute {name!r}")

    def __setattr__(self, name, value):
        raise AttributeError(f"Cant set attribute {name!r}")

    def __delattr__(self, name):
        raise AttributeError(f"Cannot delete attribute {name!r}")

    def __repr__(self):

        return "{}({})".format(
            type(self).__name__,
            ", ".join(
                "{k}={v}".format(
                    k=k[1:],
                    v=v,
                )
                for k, v in self.__dict__.items()
            ),
        )

    def _return_args(self):

        return self.__dict__["_args"]

    def _return_kargs(self):

        return {f"{k[1:]}": v for k, v in self.__dict__.items() if k not in ["_args"]}

    def class_private_vars(self) -> dict:
        """Method to return a dictionary with all the args and kwargs.
            Method allows the modification of the attribute values.

        Returns:
            dict
        """
        return vars(self)

    def return_class_data_as_dict(self) -> dict:
        """Method to return the payload as a dictionary. Values are immutable.

        Returns:
            dict
        """
        return {
            f"{key[1:]}": value
            for key, value in self.__dict__.items()
            if key
            not in [
                "_args",
                "_MRN",
                "_STATES",
                "_LOGGER",
                "_MasterFileJson",
                "_doh",
                "_dohs",
            ]
        }

    @staticmethod
    def parse_iso_datetime_to_hl7_format(state: str, input: str) -> datetime:
        """Method to parse ISO datetime and return the desired format for HL7 messages.

        Args:
            state (str)
            input (str)

        Returns:
            datetime
        """
        return parse_datetime_isoformat(state=state, date_time=input).strftime(
            "%Y%m%d%H%M%S"
        ) + time_zone_state_adjustment(doh=state)


class Hl7Record(SharedMethods):
    def __init__(self, record: dict, logger: logging, MasterFileJson):

        flat_dict = flatten(record)
        super().__init__(**record)

        self.class_private_vars()["_perform_facility_override"] = False
        self.class_private_vars()["_LOGGER"] = logger
        self.class_private_vars()["_MasterFileJson"] = MasterFileJson

        missing_attrs_required = []
        missing_attrs_optional = []

        # optional_params -------

        for key in HL7_OPTIONAL_ARGS:
            try:
                flat_dict[key]
            except KeyError:
                missing_attrs_optional.append(key)

        # required_params ------

        for key in HL7_REQUIRED_ARGS:
            try:
                flat_dict[key]

                if ("order", "states", "RESULTED") == key:
                    self._logger_handler(
                        logger=self.LOGGER,
                        result_date=self.order["states"]["RESULTED"],
                    )
                if ("test_kit_types", "assay") == key: #TODO do we need vaccine_kit_types as well?
                    self._logger_handler(
                        logger=self.LOGGER, assay=self.test_kit_types["assay"]
                    )

                if ("facility", "org_id") == key:
                    self._logger_handler(
                        logger=self.LOGGER, org_id=self.facility["org_id"]
                    )

                    if self.LOGGER is not None:
                        self.LOGGER.info("Determining DOH.")

                    dohs = self.MasterFileJson.find_dohs(
                        facility_org_id=self.facility["org_id"]
                    )
                    self.class_private_vars()["_dohs"] = dohs
                    self._logger_handler(
                        logger=self.LOGGER, doh=[doh.title() for doh in dohs]
                    )

            except KeyError:
                missing_attrs_required.append(key)

        for dict in HL7_REQUIRED_ARGS_IN_LISTS:

            try:
                for key, value_to_check in dict.items():

                    nested_list = flat_dict[key]

                    for item in nested_list:
                        assert value_to_check in item
            except AssertionError:
                missing_attrs_required.append((key, value_to_check))

        if missing_attrs_optional:
            logger.warning(
                f"Object is missing the following optional attributes: {dumps(['.'.join(i) for i in missing_attrs_optional])}"
            )

        assert (
            len(missing_attrs_required) == 0
        ), f"Object is missing the following required attributes: {dumps(['.'.join(i) for i in missing_attrs_required])}"

        self.class_private_vars()["_MRN"] = (
            self.order["patient_id"][:5] + "-" + self.order["patient_id"][5:]
        )
        self._logger_handler(logger=self.LOGGER, mrn=self.MRN)

        if self._message_delivered():

            if not PROCESS_REPEATED_MESSAGES:
                raise RepeatedHl7MessageError("Message has been delivered already.")

    def _message_delivered(self) -> bool:
        if "HL7_SENT" in self.order["states"]:
            return self.order["states"]["HL7_SENT"] is not None
        else:
            return False

    @staticmethod
    def _logger_handler(logger, **kargs):
        logger.append_keys(**kargs) if logger is not None else 1

    def _is_state_valid(self) -> bool:
        return (
            findStateAbbreviation(self.get_patient_data("address", "state"))
            in self.STATES
        )

    def _return_data_if_state_valid(self, input: str) -> str:
        return input if self._is_state_valid() else ""

    @staticmethod
    def _parse_flat_dict(collection, *args):
        try:
            return flatten(collection)[args]
        except KeyError:
            return "^"

    @staticmethod
    def _function_mappers(function_name: str) -> Callable:
        """Function to emulate switch function. Maps the settings specified in the JSON file and the respective function.

        Args:
            function_name (str)

        Raises:
            KeyError: If a function is not mapped.

        Returns:
            Callable
        """
        mapped_functions = {
            "stop_negative": state_level_validation_funcs.stop_negative,
            "perform_facility_override": state_level_validation_funcs.perform_facility_override,
        }

        try:
            return mapped_functions[function_name]
        except KeyError:
            raise KeyError("Function is not mapped")

    def is_message_required(
        self,
    ) -> bool:
        """Method to test if the message meets the conditions to create HL7 message.

        Returns:
            bool
        """
        test_logic = [
            any(
                clean_str(result["result"]) not in ["positive", "negative"]
                for result in self.order["results"]
            ),
            any(result == "" for result in self.order["results"]),
            any(result is None for result in self.order["results"]),
        ]

        return not any(test_logic)

    def add_doh(self, input: str) -> None:
        self.class_private_vars()["_doh"] = clean_str(input)

    def facility_name(self):
        return prepend_nomi_to_facility_name(doh=self.doh) + self.facility["name"]

    def patient_optional_address(self):

        if self.doh in ADD_COUNTIES:

            if "county" in self.patient["address"]:
                return f'^^^^{self.patient["address"]["county"]}'

        return ""

    def str_collection_date_time(self) -> str:
        return self.parse_iso_datetime_to_hl7_format(
            state=self.doh, input=self.order["sample_date"]
        )

    def str_results_date_time(self) -> str:
        return self.parse_iso_datetime_to_hl7_format(
            state=self.doh, input=self.order["states"]["RESULTED"]
        )

    def str_patient_dob(self):
        return datetime.strptime(self.patient["personal"]["dob"], "%Y-%m-%d").strftime(
            "%Y%m%d"
        )

    def patient_phone_number(self) -> str:
        if "phone" in self.patient["contact"]:

            phone_number_list = [
                number
                for number in self.patient["contact"]["phone"]
                if number.isdigit()
            ]

            phone_number = "".join(phone_number_list[-10:])

            area_code = "" if len(phone_number_list) < 10 else phone_number[:3]
            digit_number = (
                phone_number[-7:] if len(phone_number_list) < 10 else phone_number[3:]
            )

            return area_code + "^" + digit_number

        else:
            return "^"

    def get_optional_patient_personal_info(self, field: str):
        if field not in ["ethnicity", "race", "ssn"]:
            raise ValueError(f"Field: {field} is not valid")

        return self.patient["personal"].get(field, "")

    def patient_state(self) -> str:
        return findStateAbbreviation(
            self._return_data_if_state_valid(
                input=self.get_patient_data("address", "state")
            )
        )

    def patient_postal_code(self) -> str:
        return self._return_data_if_state_valid(
            input=self.get_patient_data("address", "postal_code")
        )

    def get_test_kit_id(self) -> str:
        return self._parse_flat_dict(self.order, "test_kit_id")

    def get_vaccine_kit_id(self) -> str:
        return self._parse_flat_dict(self.order, "vaccine_kit_type_id")

    def get_mrn(self) -> str:
        return self.MRN

    def get_facility_data(self, *args) -> str:
        return self._parse_flat_dict(self.facility, *args)

    def get_patient_data(self, *args) -> str:
        return self._parse_flat_dict(self.patient, *args)

    def get_patient_id(self) -> str:
        return self.encounter["patient_id"]

    def get_patient_address(self, street_field: int = 1):

        if street_field == 1:
            return rm_characters(self.patient["address"]["street_1"])
        elif street_field == 2:
            return (
                rm_characters(self.patient["address"]["street_2"])
                if "street_2" in self.patient["address"]
                else ""
            )
        else:
            raise ValueError("Only valid for street 1 or 2.")

    def get_ordering_facility_NPI(self, default: str):
        return self.facility["npi"] if "npi" in self.facility else default

    def performing_facility_name(self):

        if self.perform_facility_override:
            return self.facility["default_pcr_lab_id"]

        return "NOMI"


    def performing_facility_address_1(self):
        if self.perform_facility_override:
            return self.facility["address"]["street_1"]
        return "1151 E 3900 S"

    def performing_facility_address_2(self):
        if self.perform_facility_override:
            try:
                return self.facility["address"]["street_2"]
            except KeyError:
                return ""

        return "UNIT B"

    def performing_facility_city(self):
        if self.perform_facility_override:
            return self.facility["address"]["city"]
        return "SALT LAKE CITY"

    def performing_facility_state(self):
        if self.perform_facility_override:
            return self.facility["address"]["state"]
        return "UT"

    def performing_facility_zip(self):
        if self.perform_facility_override:
            return self.facility["address"]["postal_code"]
        return "84124"

    def performing_facility_country(self):
        if self.perform_facility_override:
            return self.facility["address"]["country"]
        return "USA"

    def performing_facility_override(self, value: bool):
        self.class_private_vars()["_perform_facility_override"] = value
        
    def facility_clia_number(self):
        if self.order["procedure_type_id"] in ["PCR"]:
            return "46D2189468"
        return self.facility["clia_id"]
        
    def procedure_type_id(self):
        return self.order["procedure_type_id"] 

    def state_config_validation(self, state: str) -> Union[dict, None]:
        """Function to emulate switch function. Maps the states to the specified validations. If a state does not have validation, returns None. If a stage meets a condition, returns the message as a dictionary.

        Args:
            state (str)
            stream_message (dict)

        Returns:
            Union[dict, None]
        """

        state_clean = clean_str(state)

        messages = []
        validation_outputs = []

        try:
            validation_funcs = VALIDATION_MAPPERS[state_clean]
        except KeyError:
            pass
        else:

            for result in self.order["results"]:

                result_name = clean_str(result["result_name"])

                try:

                    func_names_key = list(
                        filter(lambda x: result_name in x, validation_funcs)
                    )
                    assert len(func_names_key) == 1

                    func_names_key = func_names_key[0]

                except AssertionError:
                    continue

                for validation_function_name in validation_funcs[func_names_key]:

                    validation_output, message = self._function_mappers(
                        validation_function_name
                    )(
                        self, state, result
                    )  # Functions should always be state + data needed for cuntion else we need

                    validation_outputs.append(validation_output)
                    messages.append(message)

        validation_outputs = [i for i in validation_outputs if i is not None]
        messages = set(str(i) for i in messages if i is not None)

        if validation_outputs:
            if all(validation_outputs):  # Empty [] returns True
                return messages


class MasterFileJson(SharedMethods):
    """Class to represent the Json Master File.

    Args:
        SharedMethods
    """

    def __init__(self, master_json_dir_path: str = "json"):

        with open(
            path_join(master_json_dir_path, "master_file.json")
        ) as file:  # change this to handle text and streaming
            json_data = load(file)

            super().__init__(**json_data)

    def find_dohs(self, facility_org_id: str) -> list:
        """Method to find the DOHs mapped to the specific message facility_clia.

        Args:
            facility_org_id (str)

        Returns:
            list
        """
        doh_list = list(
            map(
                lambda x: clean_str(x["doh"]),
                filter(lambda x: facility_org_id in x["orgList"], self.doh_mappings),
            )
        )

        return doh_list


class StateDoh:
    """Class to store Multiple DOH information."""

    def __init__(self, dohs: list):
        """Constructor takes a list of DOHs names and gets the data from the
        json files stored under the json/ directory as dictionaries under the
        doh_data attribute.

        Constructor extracts important information from the dictionaries and the
        information is stored in doh_logic, file_locations and file_formats attributes.

        Args:
            dohs (list)
        """
        self._dohs = dohs

        self.doh_data = {}
        self.doh_logic = {}
        self.file_locations = {}
        self.file_formats = {}

        for doh in dohs:

            with open(path_join("json", f"{doh.title()}.json")) as file:
                self.doh_data[doh] = load(file)
                self.doh_data[doh]["metadata"] = {
                    "message_control_id": uuid4().int,
                    "message_timestamp": SharedMethods.parse_iso_datetime_to_hl7_format(
                        state=doh, input=datetime.utcnow()
                    ),
                }

                doh_logic = self.doh_data[doh]["logic"]
                self.file_locations[doh] = doh_logic["file_location"]
                self.file_formats[doh] = doh_logic["file_format"]
