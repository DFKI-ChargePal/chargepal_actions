#!/usr/bin/env python3
import rospy
import sqlite3
from typing import List, Tuple, Dict, Optional
import requests
from datetime import datetime
import hashlib
import base64

robot_name = rospy.get_param("/robot_name")
mir_address = rospy.get_param("/mir_address")
mir_user = rospy.get_param("/mir_user_name")
mir_password = rospy.get_param("/mir_user_password")
log_file_path = rospy.get_param("/log_file_path")

from chargepal_services.srv import (
    clearMirError,
    deleteMirMission,
)


def extract_row_contents(
    db_cursor: sqlite3.Cursor,
    table_name: str,
    selected_columns: str,
    row_name: str,
) -> Optional[Tuple]:
    """
    Extracts the contents of a row from a specified table in the database.

    Args:
        db_cursor (sqlite3.Cursor): The cursor object for executing SQL queries.
        table_name (str): The name of the table to extract the row from.
        selected_columns (str): The columns to select from the table.
        row_name (str): The name of the row to extract.

    Returns:
        Optional[Tuple]: The contents of the row as a tuple, or None if the row is not found.
    """
    table_name = (
        "cart_info" if table_name == "battery_action_info" else table_name
    )  # since battery_action_info table is not present in rdb

    db_cursor.execute(
        f"SELECT {selected_columns} FROM {table_name} WHERE name=?",
        (row_name,),
    )

    row = db_cursor.fetchone()

    if row is None:
        rospy.logwarn(f"Row with name '{row_name}' not found in table '{table_name}'")

    return row


def fetch_column_names(cursor: sqlite3.Cursor, table_name: str) -> list:
    """
    Fetches the column names of a given table in a SQLite database.

    Args:
        cursor (sqlite3.Cursor): The cursor object used to execute SQL queries.
        table_name (str): The name of the table.

    Returns:
        list: A list of column names.

    """
    cursor.execute(
        f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    table_definition = cursor.fetchone()[0]
    column_names = [
        part.split()[0] for part in table_definition.split("(")[1].split(",")
    ]
    return column_names


def clear_table_values(
    table_parameters: Tuple[str, Tuple[str]], cursor: sqlite3.Cursor
):
    """
    Clears all rows from the specified tables.

    Args:
        table_parameters (Tuple[str, Tuple[str]]): A list of table names and their corresponding parameters.
        cursor (sqlite3.Cursor): The cursor object used to execute SQL queries.

    Returns:
        None
    """
    for param in table_parameters:
        table_name = param.table
        cursor.execute(f"DELETE FROM {table_name}")


def clear_mir_error() -> bool:
    """
    Clears the error state of the MiR robot.

    This function waits for the "/mir_rest_api/clear_error" service to become available and then attempts to clear the error state of the MiR robot.
    It retries up to 3 times if the service call fails due to a `rospy.ServiceException`.

    Returns:
        bool: True if the error state was successfully cleared, False otherwise.
    """
    rospy.wait_for_service("/mir_rest_api/clear_error")
    success = False
    retry_counter = 0
    while not success and retry_counter <= 3:
        try:
            service_proxy_clear_mir_error = rospy.ServiceProxy(
                "/mir_rest_api/clear_error", clearMirError
            )
            response = service_proxy_clear_mir_error(robot_name)
            success = response.success
            enter_log_file(f"    Clearing MiR error state - {success}")

        except rospy.ServiceException as e:
            retry_counter += 1
            enter_log_file(
                f"    chargepal_actions_ERROR:Unable to clear mir error. Error is {e} "
            )

    return success


def reset_mission_queue() -> bool:
    """
    Resets the mission queue by calling the Mir REST API service to delete the mission queue.

    Returns:
        bool: True if the mission queue was successfully reset, False otherwise.
    """
    rospy.wait_for_service("/mir_rest_api/delete_mission_queue")
    success = False
    retry_counter = 0
    while not success and retry_counter <= 10:
        try:
            service_proxy_reset_mission_queue = rospy.ServiceProxy(
                "/mir_rest_api/delete_mission_queue", deleteMirMission
            )
            response = service_proxy_reset_mission_queue(robot_name)
            success = response.success
            enter_log_file(f"    Clearing MiR error state - {success}")

        except rospy.ServiceException as e:
            retry_counter += 1
            enter_log_file(
                f"    chargepal_actions_ERROR:Unable to reset mir mission queue. Error is {e} "
            )

    return success


def get_mir_missions_group_guid(headers: Dict[str, str], api_url: str):
    """
    Retrieves the mission group GUID for the "Chargepal" mission group.

    Args:
        headers (Dict[str, str]): The headers to be included in the API request.
        api_url (str): The base URL of the API.

    Returns:
        str: The GUID of the "Chargepal" mission group, if found. None otherwise.
    """
    missions = requests.get(api_url + "/mission_groups", headers=headers)
    if missions.status_code == 200:
        data = missions.json()
        for item in data:
            if item["name"] == "Chargepal":
                mission_group_guid = item["guid"]
                return mission_group_guid
    return None


def get_mir_mission_guid(
    headers: Dict[str, str], api_url: str, mission_group_guid: str, mission_name: str
):
    """
    Retrieves the mission GUID for a given mission name within a mission group.

    Args:
        headers (Dict[str, str]): The headers to be included in the request.
        api_url (str): The base URL of the API.
        mission_group_guid (str): The GUID of the mission group.
        mission_name (str): The name of the mission.

    Returns:
        str: The GUID of the mission if found, None otherwise.
    """
    missions = requests.get(
        api_url + "/mission_groups/" + mission_group_guid + "/missions",
        headers=headers,
    )
    if missions.status_code == 200:
        data = missions.json()

        for item in data:
            if item["name"] == mission_name:
                mission_guid = item["guid"]
                return mission_guid
    return None


def post_mission(
    headers_: Dict[str, str], api_url: str, mission_id: str, description: str
) -> str:
    """
    Posts a mission to the specified API URL.

    Args:
        headers_: A dictionary containing the headers for the API request.
        api_url: The URL of the API endpoint.
        mission_id: The ID of the mission.
        description: The description of the mission.

    Returns:
        The ID of the posted mission.

    Raises:
        None
    """
    mission_data = {
        "mission_id": mission_id,
        "message": description,
        "parameters": [],
        "priority": 0,
        "description": description,
    }

    response = requests.post(
        api_url + "/mission_queue", json=mission_data, headers=headers_
    )
    if response.status_code == 201:
        data = response.json()
        id_ = data["id"]
        return id_


def get_state(headers: Dict[str, str], api_url: str, id_: int) -> Tuple[bool, str]:
    """
    Retrieves the state of a mission and the system from the given API.

    Args:
        headers (Dict[str, str]): The headers to be included in the API request.
        api_url (str): The URL of the API.
        id_ (int): The ID of the mission.

    Returns:
        Tuple[bool, str]: A tuple containing a boolean value indicating whether the mission is done or not,
        and a string representing the system state.
    """
    mission_state = requests.get(
        api_url + "/mission_queue/" + str(id_), headers=headers
    )
    system_state = requests.get(api_url + "/status", headers=headers)

    if mission_state.status_code == 200 and system_state.status_code == 200:
        mission_data = mission_state.json()
        system_state = system_state.json()

        if mission_data["state"] == "Done":
            return True, system_state["state_text"]
        else:
            return False, system_state["state_text"]


def enter_log_file(message: str):
    """
    Writes the given message along with a timestamp to the log file.

    Args:
        message (str): The message to be written to the log file.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file_path, "a") as log_file:
        log_file.write(f"{timestamp} - {message}\n")


def mir_rest_api_initialise() -> Tuple[Dict[str, str], str]:
    """
    Initializes the REST API connection with the MiR Robot.

    Returns:
        A tuple containing the headers (a dictionary of HTTP headers) and the MiR API URL.
    """
    username = mir_user
    password = mir_password
    auth_string = f"{username}:{hashlib.sha256(password.encode()).hexdigest()}"
    base64_auth_string = base64.b64encode(auth_string.encode()).decode("utf-8")
    mir_api_url = "http://" + mir_address + "/api/v2.0.0"
    headers = {
        "Content-Type": "application/json",
        "Accept-Language": "en_US",
        "Authorization": f"Basic {base64_auth_string}",
    }

    return headers, mir_api_url
