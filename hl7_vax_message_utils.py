from pathlib import Path
from string import Template
from typing import Callable
from datetime import datetime
from aws_lambda_powertools import Logger
from dsl_utils.utils import path_join
from dsl_utils.utils import clean_str


class Hl7Record:
    pass


TEMPLATE_BASE = Path("vax_templates")
MSH_TEMPLATE = "msh.txt"
OBX_TEMPLATE = "obx.txt"
ORC_TEMPLATE = "orc.txt"
PD1_TEMPLATE = "pd1.txt"
PID_TEMPLATE = "pid.txt"
RXA_TEMPLATE = "rxa.txt"
RXR_TEMPLATE = "rxr.txt"

logger = Logger(service="hl7_vax_message_utils")


def apply_ssn_logic(ssn: str, doh_json: dict) -> str:
    """Function to control representation of missing social security numbers in HL7 messages.

    Args:
        ssn (str)
        doh_json (dict)

    Returns:
        str
    """
    return (
        ""
        if ssn == "" or not doh_json["specific_values"]["include_ssn"]
        else ssn + "^SS"
    )


def loadFileTemplate(fileName: str) -> str:
    """Function to load the template to be used by the Template Object.

    Args:
        fileName (str)

    Returns:
        str
    """
    with open(path_join(TEMPLATE_BASE, fileName), "r") as file:
        return file.read()


def imprintTemplate(template_name: str, value_dict: dict) -> Template:
    """Function to populate fields in the Template Object.

    Args:
        template_name (str)
        value_dict (dict)

    Returns:
        Template
    """
    sectionTemplate = loadFileTemplate(template_name)
    hl7Section = Template(sectionTemplate).substitute(value_dict)

    return hl7Section


def patient_table_mapper(
    value_to_check: str,
    table_to_iterate: list,
    return_key: str,
    comparison_operator: str,
    default_value: str = None,
    raise_error: Exception = None,
    json_key: str = "databus_name",
) -> str:
    """Function to map different tables to the argument value_to_check.

    Args:
        value_to_check (str)
        table_to_iterate (list)
        return_key (str)
        comparison_operator (str)
        default_value (str, optional). Defaults to None.
        raise_error (Exception, optional). Defaults to None.
        json_key (str, optional). Defines the key to use to filter the differnt json files. Defaults to "databus_name".

    Raises:
        raise_error: If no condition is met, an optional error can be raised.
        ValueError: If no condition is met and no default values or errors are given.

    Returns:
        str
    """
    for test in table_to_iterate:

        if clean_str(comparison_operator) == "in":
            if clean_str(value_to_check).replace(" ", "") in [
                clean_str(_).replace(" ", "") for _ in test[json_key]
            ]:
                return test[return_key]

        if clean_str(comparison_operator) == "==":
            if clean_str(value_to_check).replace(" ", "") == clean_str(
                test[json_key]
            ).replace(" ", ""):
                return test[return_key]

    if raise_error is not None:
        raise raise_error
    elif default_value is not None:
        return default_value
    else:
        raise ValueError("Not raise error or default value specified.")


def convertPatientEthnicity(
    ethnicity: str, doh_json: dict, mater_file_ethnicity_table: list
) -> str:
    return patient_table_mapper(
        value_to_check=ethnicity.lower(),
        table_to_iterate=mater_file_ethnicity_table,
        return_key="value",
        default_value=doh_json["specific_values"]["default_ethnicity_code"],
        comparison_operator="in",
    )


def convertPatientEthnicityDesc(
    ethnicity: str, doh_json: dict, mater_file_ethnicity_table: list
) -> str:
    return patient_table_mapper(
        value_to_check=ethnicity.lower(),
        table_to_iterate=mater_file_ethnicity_table,
        return_key="desc",
        default_value=doh_json["specific_values"]["default_ethnicity_desc"],
        comparison_operator="in",
    )


def convertPatientRace(race: str, doh_json: dict, master_file_race_table: list) -> str:
    return patient_table_mapper(
        value_to_check=race.lower(),
        table_to_iterate=master_file_race_table,
        return_key="value",
        default_value=doh_json["specific_values"]["default_race_code"],
        comparison_operator="in",
    )


def convertPatientRaceDesc(
    race: str, doh_json: dict, master_file_race_table: list
) -> str:
    return patient_table_mapper(
        value_to_check=race.lower(),
        table_to_iterate=master_file_race_table,
        return_key="desc",
        default_value=doh_json["specific_values"]["default_race_desc"],
        comparison_operator="in",
    )


#TODO: Done, but will need the actual org_ids from Jovi
def createMSHBlock(data: Hl7Record, doh_json: dict) -> Template:
    message_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    if data["org_ID"] == 2: #MDC
        login_id = "MGW36685^Nomi Health, Inc"
    elif data["org_ID"] == 6: #famu
        login_id = "BCJ72636^FLORIDA A&M UNIVERSITY SHS"
    elif data["org_ID"] == 789: #Amazon
        login_id = "MGW36685^Nomi Health, Inc"
    else:
        login_id = doh_json["specific_values"]["login_id"]

    msh_dict = {
        "msh3": doh_json["specific_values"]["msh3"],
        "login_id": login_id,
        "msh5": doh_json["specific_values"]["msh5"],
        "msh6": doh_json["specific_values"]["msh6"],
        "msh15": doh_json["specific_values"]["msh15"],
        "msh16": doh_json["specific_values"]["msh16"],
        "msh4_3": doh_json["specific_values"]["msh4_3"],
        "msh21_1": doh_json["specific_values"]["msh21_1"],
        "message_control_id": data.get_mrn(),
        "message_timestamp": message_timestamp,
    }

    return imprintTemplate(MSH_TEMPLATE, msh_dict)


def createOBXBlock(data: Hl7Record, doh_json: dict) -> Template:  # One Block per Result
    obx_dict = {
        "vaccine_date": data["Vaccine Admin Date"], # TODO: need to check what this is in the payload
        "obx2": doh_json["specific_values"]["obx2"],
        "obx3": doh_json["specific_values"]["obx3"],
        "obx4": doh_json["specific_values"]["obx4"],
        "obx5": doh_json["specific_values"]["obx5"],
        "obx6_1": doh_json["specific_values"]["obx6_1"],
        "obx6_2": doh_json["specific_values"]["obx6_2"],
        "obx6_3": doh_json["specific_values"]["obx6_3"],
    }

    return imprintTemplate(OBX_TEMPLATE, obx_dict)


def createORCBlock(data: Hl7Record, doh_json: dict) -> Template:
    orc_dict = {
        "order_number": data.get_vaccine_kit_id(),
        "message_control_id": data.get_mrn(),
        "orc3_2": doh_json["specific_values"]["orc3_2"],
        "orc4_2": doh_json["specific_values"]["orc4_2"],
        "checkedinby": data['checkedinby'], # TODO this will be the nurses ID
        "clinician_first": data["clinician_first"], # TODO need this from payload
        "clinician_last": data["clinician_last"], # TODO need this from payload
    }

    return imprintTemplate(ORC_TEMPLATE, orc_dict)


def createPD1Block(data):
    """This is used currently only for TX."""
    pd1_dict = dict()
    date_time_string = data["Vaccine Administered Date/Time"] #TODO going to need to check this
    timestamp = datetime.strptime(date_time_string, "%Y-%m-%dT%H:%MZ")
    pd1_dict["Protection_Indicator"] = timestamp.strftime("%Y%m%d")
    return imprintTemplate(PD1_TEMPLATE, pd1_dict)


def createPIDBlock(data: Hl7Record, doh_json: dict, master_file_obj) -> Template:
    """Generates a patient identification block by imprinting values from the data frame into a string
    template that is loaded from the file system
    """

    try:
        segment = "patient_race"
        patient_race = convertPatientRace(
            race=data.get_optional_patient_personal_info(field="race"),
            doh_json=doh_json,
            master_file_race_table=master_file_obj.race_table,
        )
        segment = "patient_race_desc"
        patient_race_desc = convertPatientRaceDesc(
            race=data.get_optional_patient_personal_info(field="race"),
            doh_json=doh_json,
            master_file_race_table=master_file_obj.race_table,
        )
        segment = "patient_ethnicity"
        patient_ethnicity = convertPatientEthnicity(
            ethnicity=data.get_optional_patient_personal_info(field="ethnicity"),
            doh_json=doh_json,
            mater_file_ethnicity_table=master_file_obj.ethnicity_table,
        )
        segment = "patient_ethnicity_desc"
        patient_ethnicity_desc = convertPatientEthnicityDesc(
            ethnicity=data.get_optional_patient_personal_info(field="ethnicity"),
            doh_json=doh_json,
            mater_file_ethnicity_table=master_file_obj.ethnicity_table,
        )
    except Exception as e:
        raise type(e)(f"Failed extracting variable - var: {segment}. Error:{e}")

    pid_dict = {
        "patient_mrn": data.get_mrn(),
        "pid4_2": doh_json["specific_values"]["pid4_2"],
        "pid4_7": doh_json["specific_values"]["pid4_7"],
        "patient_last": data.get_patient_data("personal", "last_name"),
        "patient_first": data.get_patient_data("personal", "first_name"),
        "patient_mi": data.get_patient_data("personal", "middle_name"),
        "patient_dob": data.str_patient_dob(),
        "patient_gender": data.get_patient_data("personal", "gender"),
        "patient_race": patient_race,
        "patient_race_desc": patient_race_desc,
        "pid11_3": doh_json["specific_values"]["pid11_3"],
        "patient_address_1": data.get_patient_address(),
        "patient_address_2": data.get_patient_address(street_field=2),
        "patient_address_city": data.get_patient_data("address", "city"),
        "patient_address_state": data.patient_state(),
        "patient_address_zip": data.patient_postal_code(),
        "patient_phone": data.patient_phone_number(),  # needs to look like this 888^1234264
        "patient_ethnicity": patient_ethnicity,
        "patient_ethnicity_desc": patient_ethnicity_desc,
        "pid23_3": doh_json["specific_values"]["pid23_3"],
    }

    return imprintTemplate(PID_TEMPLATE, pid_dict)


def createRXABlock(data: Hl7Record, doh_json: dict) -> Template:

    if data["org_ID"] == 2: #MDC
        site_id = "^^^7000^^^^^Nomi Health, Inc"
    elif data["org_ID"] == 6: #famu
        site_id = "^^^8000^^^^^FLORIDA A&M UNIVERSITY SHS"
    elif data["org_ID"] == 789: #Amazon
        site_id = "^^^7000^^^^^Nomi Health, Inc"
    else:
        site_id = doh_json["specific_values"]["site_id"]

    rxa_dict = {
        "procedure_date": data["procedure_date"],
        "cvx_code": data["cvx_code"], # TODO: need to make sure these are in the data.
        "cvx_description": data["cvx_description"],
        "rxa6_3": doh_json["rxa6_3"],
        "rxa6_4": doh_json["rxa6_4"],
        "rxa8": doh_json["rxa8"],
        "vis_description": data["vis_description"],
        "rxa6_6": doh_json["rxa6_6"],
        "clinician_first": data["clinician_first"],
        "clinician_last": data["clinician_last"],
        "site_id": site_id,
        "lot_Number": data["lot_Number"],
        "lot_expiration_date": data["lot_expiration_date"],
        "mfg_code": data["mfg_code"],
        "vax_manufacturer": data["vax_manufacturer"],
        "rxa22": doh_json["rxa22"],
        "report_date": data["report_date"],
    }

    return imprintTemplate(RXA_TEMPLATE, rxa_dict)


def createRXRBlock(data: Hl7Record, doh_json: dict) -> Template:

    if data["org_id"] in [2, 6]: #FL org_ids
        admin_description = ""
        location_description = ""
    else:
        admin_description = data["admin_description"]
        location_description = data["location_description"]

    rxr_dict = {
        "admin_code": data["admin_code"],
        "admin_description": admin_description,
        "rxr2_3": doh_json["rxr2_3"],
        "location_code": data["location_code"],
        "location_description": location_description,
        "rxr3_3": doh_json["rxr3_3"],
    }

    return imprintTemplate(RXR_TEMPLATE, rxr_dict)


# ****************************************************************************
def hl7_message_blocks_switch(segment_type: str, *args, **kargs) -> Callable:
    """Function to emulate switch statment for the different block functions.

    Args:
        segment_type (str)

    Raises:
        KeyError: if the segment_type is not mapped.

    Returns:
        Callable
    """
    segment_functions = {
        "MSH": createMSHBlock,
        "OBX": createOBXBlock,
        "ORC": createORCBlock,
        "PID": createPIDBlock,
        "RXA": createRXABlock,
        "RXR": createRXRBlock,
    }

    try:
        func = segment_functions[segment_type]
    except KeyError as e:
        raise KeyError("HL7 message block function not implemented.")
    else:
        return func(*args, **kargs)


def section_requirements(
    segment: str, data: Hl7Record, doh_json: dict, master_file_obj
) -> dict:
    """Function to build the arguments for the different block functions.

    Args:
        segment (str)
        data (dict)
        doh_json (dict)
        master_file_obj (_type_)

    Returns:
        dict
    """
    #TODO might need to add PD1 here.
    data_args = ["PID", "ORC", "OBX"]
    master_file_obj_args = ["PID", "OBX"]
    doh_json_args = ["MSH", "PID", "ORC", "OBX"]

    kargs = {}

    if segment in data_args:
        kargs["data"] = data

    if segment in doh_json_args:
        kargs["doh_json"] = doh_json

    if segment in master_file_obj_args:
        kargs["master_file_obj"] = master_file_obj

    return kargs


def create_message(data: Hl7Record, doh_json: dict, master_file_obj) -> str:
    """Function to create HL7 messages.

    Args:
        data (dict)
        doh_json (dict)
        master_file_obj (_type_)

    Raises:
        Exception: Catches error and raises Exception.

    Returns:
        str
    """
    required_segments = doh_json["segment_list"]
    segment_list = []

    try:
        for seg in required_segments:

            kargs = section_requirements(
                segment=seg,
                data=data,
                doh_json=doh_json,
                master_file_obj=master_file_obj,
            )

            segment = hl7_message_blocks_switch(segment_type=seg, **kargs)

            segment_list.append(segment)

        str_msg = "".join(segment_list)

    except Exception as e:
        raise Exception(f"Error building HL7 message - segment: {seg}: {e}")
    else:
        return str_msg
