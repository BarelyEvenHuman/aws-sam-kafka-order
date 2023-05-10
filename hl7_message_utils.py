from pathlib import Path
from string import Template
from typing import Callable

from aws_lambda_powertools import Logger
from dsl_utils.utils import path_join
from dsl_utils.utils import clean_str


class Hl7Record:
    pass


TEMPLATE_BASE = Path("templates")
MSH_TEMPLATE = "msh.txt"
SFT_TEMPLATE = "sft.txt"
NTE_TEMPLATE = "nte.txt"
OBR_TEMPLATE = "obr.txt"
OBX_TEMPLATE = "obx.txt"
ORC_TEMPLATE = "orc.txt"
PID_TEMPLATE = "pid.txt"
SPM_TEMPLATE = "spm.txt"

logger = Logger(service="hl7_message_utils")


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


def master_json_result_mapper(
    result_value: str,
    result_name: str,
    procedure_type_ids: str,
    master_file_results_table: list,
    default_value: str,
    return_key: str,
):

    result_value = clean_str(result_value)
    result_name = clean_str(result_name)
    procedure_type_ids = clean_str(procedure_type_ids[0])

    if result_value in ["positive", "negative"]:

        if result_name in ["c19", "monkeypox"]:
            return master_file_results_table[result_value][result_name][return_key]
        else:
            return master_file_results_table[result_value][result_name][
                procedure_type_ids
            ][return_key]
    else:
        return default_value


# ****************** Json Master File and DOH Json mappers ***********************
def map_abnormal_flag(
    result_value: str,
    result_name: str,
    procedure_type_ids: str,
    master_file_results_table: list,
) -> str:
    return master_json_result_mapper(
        result_value=result_value,
        result_name=result_name,
        procedure_type_ids=procedure_type_ids,
        master_file_results_table=master_file_results_table,
        default_value="N",
        return_key="abnormal_flag",
    )


def map_abnormal_desc(
    result_value: str,
    result_name: str,
    procedure_type_ids: str,
    master_file_results_table: list,
) -> str:
    return master_json_result_mapper(
        result_value=result_value,
        result_name=result_name,
        procedure_type_ids=procedure_type_ids,
        master_file_results_table=master_file_results_table,
        default_value="Normal",
        return_key="abnormal_desc",
    )


def map_result_description(
    result_value: str,
    result_name: str,
    procedure_type_ids: str,
    master_file_results_table: list,
) -> str:
    return master_json_result_mapper(
        result_value=result_value,
        result_name=result_name,
        procedure_type_ids=procedure_type_ids,
        master_file_results_table=master_file_results_table,
        default_value="Invalid result",
        return_key="desc",
    )


def map_result_snomed(
    result_value: str,
    result_name: str,
    procedure_type_ids: str,
    master_file_results_table: list,
) -> str:
    return master_json_result_mapper(
        result_value=result_value,
        result_name=result_name,
        procedure_type_ids=procedure_type_ids,
        master_file_results_table=master_file_results_table,
        default_value="455371000124106",
        return_key="snomed",
    )


def LOINC_Code(assay: str, doh_test_list: list, result_name: str) -> str:
    loinc_code_data = patient_table_mapper(
        value_to_check=assay,
        table_to_iterate=doh_test_list,
        return_key="loinc_code",
        raise_error=Exception("UNABLE TO MAP TEST TO LOINC CODE: " + assay),
        comparison_operator="==",
        json_key="assay",
    )

    if isinstance(loinc_code_data, dict):
        return loinc_code_data[clean_str(result_name)]

    return loinc_code_data


def observ_method(assay: str, doh_test_list: list) -> str:
    return patient_table_mapper(
        value_to_check=assay,
        table_to_iterate=doh_test_list,
        return_key="obs_method",
        raise_error=Exception("UNABLE TO MAP TEST TO OBSERVATION METHOD: " + assay),
        comparison_operator="==",
        json_key="assay",
    )


def specimen_type(assay: str, doh_test_list: list) -> str:
    return patient_table_mapper(
        value_to_check=assay,
        table_to_iterate=doh_test_list,
        return_key="spec_type",
        raise_error=Exception("UNABLE TO MAP TEST TO SPECIMEN TYPE: " + assay),
        comparison_operator="==",
        json_key="assay",
    )


def specimen_source(assay: str, doh_test_list: list) -> str:
    return patient_table_mapper(
        value_to_check=assay,
        table_to_iterate=doh_test_list,
        return_key="spec_source",
        raise_error=Exception("UNABLE TO MAP TEST TO SPECIMEN SOURCE: " + assay),
        comparison_operator="==",
        json_key="assay",
    )


def specimen_source_obr(assay: str, doh_test_list: list) -> str:
    return patient_table_mapper(
        value_to_check=assay,
        table_to_iterate=doh_test_list,
        return_key="spec_source_obr",
        raise_error=Exception("UNABLE TO MAP TEST TO SPECIMEN SOURCE OBR: " + assay),
        comparison_operator="==",
        json_key="assay",
    )


def specimen_site(assay: str, doh_test_list: list) -> str:
    return patient_table_mapper(
        value_to_check=assay,
        table_to_iterate=doh_test_list,
        return_key="site_name",
        raise_error=Exception("UNABLE TO MAP TEST TO SPECIMEN SITE: " + assay),
        comparison_operator="==",
        json_key="assay",
    )


def site_code(assay: str, doh_test_list: list) -> str:
    return patient_table_mapper(
        value_to_check=assay,
        table_to_iterate=doh_test_list,
        return_key="site_code",
        raise_error=Exception("UNABLE TO MAP TEST TO SPECIMEN SITE CODE: " + assay),
        comparison_operator="==",
        json_key="assay",
    )


def test_clia(assay: str, doh_test_list: list):
    return patient_table_mapper(
        value_to_check=assay,
        table_to_iterate=doh_test_list,
        return_key="clia_number",
        raise_error=Exception("UNABLE TO MAP TEST TO CLIA NUMBER: " + assay),
        comparison_operator="==",
        json_key="assay",
    )


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


def convertPatientEthnicitySystem(
    ethnicity: str, doh_json: dict, mater_file_ethnicity_table: list
) -> str:
    return patient_table_mapper(
        value_to_check=ethnicity.lower(),
        table_to_iterate=mater_file_ethnicity_table,
        return_key="system",
        default_value=doh_json["specific_values"]["default_ethnicity_system"],
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


def convertPatientRaceSystem(
    race: str, doh_json: dict, master_file_race_table: list
) -> str:
    return patient_table_mapper(
        value_to_check=race.lower(),
        table_to_iterate=master_file_race_table,
        return_key="system",
        default_value=doh_json["specific_values"]["default_race_system"],
        comparison_operator="in",
    )


def createSFTBlock(doh_json: dict) -> Template:

    sft_dict = {"sft_segment": doh_json["specific_values"]["sft"]}

    return imprintTemplate(SFT_TEMPLATE, sft_dict)


def createMSHBlock(doh_json: dict) -> Template:
    msh_dict = {
        "iso5": doh_json["specific_values"]["iso5"],
        "iso6": doh_json["specific_values"]["iso6"],
        "msh15": doh_json["specific_values"]["msh15"],
        "msh16": doh_json["specific_values"]["msh16"],
        "msh2": doh_json["specific_values"]["msh2"],
        "msh4_1": doh_json["specific_values"]["msh4_1"],
        "msh4_2": doh_json["specific_values"]["msh4_2"],
        "msh4_3": doh_json["specific_values"]["msh4_3"],
        "msh21_1": doh_json["specific_values"]["msh21_1"],
        "msh21_2": doh_json["specific_values"]["msh21_2"],
        "msh21_3": doh_json["specific_values"]["msh21_3"],
        "msh3": doh_json["specific_values"]["msh3"],
        "message_control_id": doh_json["metadata"]["message_control_id"],
        "message_timestamp": doh_json["metadata"]["message_timestamp"],
    }

    return imprintTemplate(MSH_TEMPLATE, msh_dict)


def createNTEBlock() -> Template:
    """Generates a notes block by imprinting values from the data frame into a string
    template that is loaded from the file system

    Returns:
        Template
    """
    return imprintTemplate(NTE_TEMPLATE, {})


def createOBRBlock(data: Hl7Record, doh_json: dict, master_file_obj) -> Template:
    """Generates a observation request block by imprinting values from the data frame into a string
    template that is loaded from the file system.

    This block should only be generated with Covid results.

    Args:
        data (dict)
        doh_json (dict)
        master_file_obj (_type_)

    Returns:
        Template
    """

    def _(order_result_value):

        try:

            segment = "loinc_code"
            loinc_code = LOINC_Code(
                assay=data.test_kit_types["assay"],
                doh_test_list=doh_json["test_list"],
                result_name=order_result_value["result_name"],
            )

            segment = "results_desc"
            results_desc = map_result_description(
                result_value=order_result_value["result"],
                result_name=order_result_value["result_name"],
                procedure_type_ids=data.test_kit_types["procedure_type_ids"],
                master_file_results_table=master_file_obj.result_table,
            )

            segment = "results_snomed"
            results_snomed = map_result_snomed(
                result_value=order_result_value["result"],
                result_name=order_result_value["result_name"],
                procedure_type_ids=data.test_kit_types["procedure_type_ids"],
                master_file_results_table=master_file_obj.result_table,
            )

            segment = "spec_source_obr"
            spec_source_obr = specimen_source_obr(
                assay=data.test_kit_types["assay"],
                doh_test_list=doh_json["test_list"],
            )

        except Exception as e:
            raise type(e)(f"Failed extracting variable - var: {segment}. Error:{e}")

        obr_dict = {
            "order_number": data.get_test_kit_id(),
            "filler_order_number": data.get_mrn(),
            "LOINC": loinc_code,
            "result_snomed": results_snomed,
            "result_date": data.str_results_date_time(),
            "result_desc": results_desc,
            "collection_date_time": data.str_collection_date_time(),
            "NPI_Number": doh_json["specific_values"]["NPI_Number"],
            "spec_source_obr": spec_source_obr,
            "provider_npi": "1891733374",
            "provider_last_name": "STEELY",
            "provider_first_name": "JUNE",
            "provider_phone_number": "385^3756419",
        }

        return imprintTemplate(OBR_TEMPLATE, obr_dict)

    return "".join(
        [
            _(clean_str(order_result_value))
            for order_result_value in data.order["results"]
            if clean_str(order_result_value["result_name"]) in ["c19", "monkeypox"]
        ]
    )


def createOBXBlock(
    data: Hl7Record, doh_json: dict, master_file_obj
) -> Template:  # One Block per Result
    """Generates a observation result block by imprinting values from the data frame into a string
    template that is loaded from the file system

    Args:
        data (dict)
        doh_json (dict)
        master_file_obj (_type_)

    Returns:
        Template
    """

    def _(order_result_value):

        try:

            segment = "loinc_code"
            loinc_code = LOINC_Code(
                assay=data.test_kit_types["assay"],
                doh_test_list=doh_json["test_list"],
                result_name=order_result_value["result_name"],
            )

            segment = "results_desc"
            results_desc = map_result_description(
                result_value=order_result_value["result"],
                result_name=order_result_value["result_name"],
                procedure_type_ids=data.test_kit_types["procedure_type_ids"],
                master_file_results_table=master_file_obj.result_table,
            )

            segment = "results_snomed"
            results_snomed = map_result_snomed(
                result_value=order_result_value["result"],
                result_name=order_result_value["result_name"],
                procedure_type_ids=data.test_kit_types["procedure_type_ids"],
                master_file_results_table=master_file_obj.result_table,
            )

            segment = "abnormal_flag"
            abnormal_flag = map_abnormal_flag(
                result_value=order_result_value["result"],
                result_name=order_result_value["result_name"],
                procedure_type_ids=data.test_kit_types["procedure_type_ids"],
                master_file_results_table=master_file_obj.result_table,
            )

            segment = "abnormal_dect"
            abnormal_dect = map_abnormal_desc(
                result_value=order_result_value["result"],
                result_name=order_result_value["result_name"],
                procedure_type_ids=data.test_kit_types["procedure_type_ids"],
                master_file_results_table=master_file_obj.result_table,
            )

            segment = "test_clia"
            test_clia_var = test_clia(
                assay=data.test_kit_types["assay"], doh_test_list=doh_json["test_list"]
            )

            segment = "obs_method"
            obs_method = observ_method(
                assay=data.test_kit_types["assay"], doh_test_list=doh_json["test_list"]
            )

        except Exception as e:
            raise type(e)(f"Failed extracting variable - var: {segment}. Error:{e}")
        obx_dict = {
            "LOINC": loinc_code,
            "collection_date_time": data.str_collection_date_time(),
            "results_date_time": data.str_results_date_time(),
            "result_desc": results_desc,
            "result_snomed": results_snomed,
            "abnormal_flag": abnormal_flag,
            "abnormal_desc": abnormal_dect,
            "abnormal_flag_suffix": doh_json["specific_values"]["abnormal_flag_suffix"],
            "NPI_Number": doh_json["specific_values"]["NPI_Number"],
            "obx_23_7": doh_json["specific_values"]["obx_23_7"],
            # "test_clia": test_clia_var,
            "test_clia":data.facility_clia_number(),
            "obs_method": obs_method,
            "site_name": data.performing_facility_name(),
            "performing_lab_street_1": data.performing_facility_address_1(),
            "performing_lab_street_2": data.performing_facility_address_2(),
            "performing_lab_city": data.performing_facility_city(),
            "performing_lab_state": data.performing_facility_state(),
            "performing_lab_zip": data.performing_facility_zip(),
            "performing_lab_country": data.performing_facility_country(),
        }

        return imprintTemplate(OBX_TEMPLATE, obx_dict)

    return "".join(
        [
            _(clean_str(order_result_value))
            for order_result_value in sorted(
                data.order["results"], key=lambda d: d["result_name"]
            )
        ]
    )


def createORCBlock(data: Hl7Record, doh_json: dict) -> Template:
    """Generates a common order block by imprinting values from the data frame into a string
    template that is loaded from the file system

    Args:
        data (dict)
        doh_json (dict)

    Returns:
        Template
    """
    orc_dict = {
        "order_number": data.get_test_kit_id(),
        "filler_order_number": data.get_mrn(),
        "provider_npi": "1891733374",
        "provider_last_name": "STEELY",
        "provider_first_name": "JUNE",
        "provider_phone_number": "385^3756419",
        "ordering_facility_name": data.facility_name(),
        "ordering_facility_NPI": data.get_ordering_facility_NPI(
            default=doh_json["specific_values"]["ordering_facility_NPI"]
        ),
        "ordering_facility_address": data.get_facility_data("address", "address"),
        "ordering_facility_city": data.get_facility_data("address", "city"),
        "ordering_facility_state": data.get_facility_data("address", "state"),
        "ordering_facility_zip": data.get_facility_data("address", "postal_code"),
        "NPI_Number": doh_json["specific_values"]["NPI_Number"],
        "order_status": doh_json["specific_values"]["order_status"],
        "ordering_facility_phone": "385^3756419",
        "ordering_provider_address": "1151 E 3900 S^UNIT B",
        "ordering_provider_city": "SALT LAKE CITY",
        "ordering_provider_state": "UT",
        "ordering_provider_zip": "84124",
    }

    return imprintTemplate(ORC_TEMPLATE, orc_dict)


def createPIDBlock(data: Hl7Record, doh_json: dict, master_file_obj) -> Template:
    """Generates a patient identification block by imprinting values from the data frame into a string
    template that is loaded from the file system

    Args:
        data (dict)
        doh_json (dict)
        master_file_obj (dict)

    Returns:
        Template
    """

    try:
        segment = "patient_ssn"
        patient_ssn = apply_ssn_logic(
            ssn=data.get_optional_patient_personal_info(field="ssn"),
            doh_json=doh_json,
        )

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
        segment = "patient_race_system"
        patient_race_system = convertPatientRaceSystem(
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

        segment = "patient_ethnicity_system"
        patient_ethnicity_system = convertPatientEthnicitySystem(
            ethnicity=data.get_optional_patient_personal_info(field="ethnicity"),
            doh_json=doh_json,
            mater_file_ethnicity_table=master_file_obj.ethnicity_table,
        )
    except Exception as e:
        raise type(e)(f"Failed extracting variable - var: {segment}. Error:{e}")

    pid_dict = {
        "patient_mrn": data.get_mrn(),
        "patient_ssn": patient_ssn,
        "pid2_suffix": doh_json["specific_values"]["pid2_suffix"],
        "patient_last": data.get_patient_data("personal", "last_name"),
        "patient_first": data.get_patient_data("personal", "first_name"),
        "patient_dob": data.str_patient_dob(),
        "ISO_Number": doh_json["specific_values"]["ISO_Number"],
        "phone_field_prefix": doh_json["specific_values"]["phone_field_prefix"],
        "patient_gender": data.get_patient_data("personal", "gender"),
        "patient_address_1": data.get_patient_address(),
        "patient_address_2": data.get_patient_address(street_field=2),
        "patient_address_city": data.get_patient_data("address", "city"),
        "patient_address_state": data.patient_state(),
        "patient_race": patient_race,
        "patient_race_desc": patient_race_desc,
        "patient_race_system": patient_race_system,
        "patient_address_zip": data.patient_postal_code(),
        "optional_address_info": data.patient_optional_address(),
        "patient_phone": data.patient_phone_number(),  # needs to look like this 888^1234264
        "patient_ethnicity": patient_ethnicity,
        "patient_ethnicity_desc": patient_ethnicity_desc,
        "patient_ethnicity_system": patient_ethnicity_system,
    }

    return imprintTemplate(PID_TEMPLATE, pid_dict)


def createSPMBlock(data: Hl7Record, doh_json: dict) -> Template:
    """Generates a specimin block by imprinting values from the data frame into a string
        template that is loaded from the file system
    Args:
        data (dict)
        doh_json (dict)

    Returns:
        Template
    """
    try:
        segment = "site_name"
        site_name = specimen_site(
            assay=data.test_kit_types["assay"], doh_test_list=doh_json["test_list"]
        )

        segment = "site_code"
        site_code_var = site_code(
            assay=data.test_kit_types["assay"], doh_test_list=doh_json["test_list"]
        )

        segment = "spec_type"
        spec_type = specimen_type(
            assay=data.test_kit_types["assay"], doh_test_list=doh_json["test_list"]
        )

        segment = "spec_source"
        spec_source = specimen_source(
            assay=data.test_kit_types["assay"], doh_test_list=doh_json["test_list"]
        )

    except Exception as e:
        raise type(e)(f"Failed extracting variable - var: {segment}. Error:{e}")

    spm_dict = {
        "order_number": data.get_patient_id(),
        "filler_order_number": data.get_mrn(),
        "Accession_Number": data.get_test_kit_id(),
        "site_name": site_name,
        "site_code": site_code_var,
        "collection_date_time": data.str_collection_date_time(),
        "received_date_time": data.str_results_date_time(),
        "spec_type": spec_type,
        "spec_source": spec_source,
    }

    return imprintTemplate(SPM_TEMPLATE, spm_dict)


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
        "SFT": createSFTBlock,
        "PID": createPIDBlock,
        "ORC": createORCBlock,
        "OBR": createOBRBlock,
        "OBX": createOBXBlock,
        "NTE": createNTEBlock,
        "SPM": createSPMBlock,
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
    no_args = ["NTE"]
    data_args = ["PID", "ORC", "OBR", "OBX", "SPM"]
    master_file_obj_args = ["PID", "OBR", "OBX"]
    doh_json_args = ["MSH", "SFT", "PID", "ORC", "OBR", "OBX", "SPM"]

    kargs = {}

    if segment in no_args:
        return kargs

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
