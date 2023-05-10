from typing import Any, Tuple
from datetime import date, datetime
from dsl_utils.utils import clean_str


def datestdtojd(stddate):
    fmt = "%Y-%m-%d"
    sdtdate = datetime.strptime(stddate, fmt)
    sdtdate = sdtdate.timetuple()
    jdate = sdtdate.tm_yday
    if len(str(jdate)) == 3:
        return jdate
    else:
        return str(jdate).zfill(3)


def year():
    currentDateTime = datetime.now()
    date = currentDateTime.date()
    year = date.strftime("%y")
    return year


def hl7_vax_file_name(state: str, order_id: str, index: int, vaccination_date: str) -> str:
    """Returns specific file name format for Vaccines.
    Example for TX: NOMIHEALTV22298.0
    Example for UT: 3GjaHP9W20220818"""
    specific_names = {
        "texas": f"NOMIHEALTV{str(year())}{str(datestdtojd(date.today().strftime('%Y-%m-%d')))}.{str(index)}",
        "utah": order_id + vaccination_date
    }
    try:
        return specific_names[state]
    except KeyError:
        return f"{order_id}"


def hl7_test_file_name(state: str, order_id: str) -> str:
    """Function to create HL7 messages file names. Some states might have custom requests, these are included here.

    Args:
        state (str): full state name in lower case.
        order_id (str)

    Returns:
        str
    """
    specific_names = {
        "texas": "NomiHealthLabServices_46D2199811"
        + "_"
        + str(date.today().strftime("%Y%m%d"))
        + "_"
        + f"{order_id}"
    }
    try:
        return specific_names[state]
    except KeyError:
        return f"{order_id}"


def perform_facility_override(self, state: str, data: Any) -> Tuple[None, None]:

    self.performing_facility_override(True)

    return None, None


def stop_negative(self, state: str, data: Any) -> Tuple[bool, dict]:
    """Function to create a message only if there are no positive results.

    Args:
        state (str): Doh Stage Name
        data (Any): Data Required for the function

    Returns:
        Tuple[bool,dict]: Bool validation outcome, dict with stage and message.
    """

    result_value = clean_str(data["result"])

    if result_value == "negative":
        return True, {"state": state, "message": "state only wants positive results"}

    return False, {}
