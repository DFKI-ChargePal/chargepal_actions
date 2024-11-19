#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Author: Gurunatraj Parthasarathy
Email: gurunatraj.parthasarathy@dfki.de
"""
import roslib
from util import *

roslib.load_manifest("chargepal_actions")
import rospy
import actionlib
import ssl
import smtplib
import rospkg
from typing import Union, Tuple, Dict
import yaml
import sqlite3
from chargepal_client.client import Grpc_Client


rospack = rospkg.RosPack()
from email.message import EmailMessage

from chargepal_actions.msg import (
    ArriveAtStationAction,
    ArriveAtStationResult,
)
from chargepal_actions.msg import (
    CallForHelpAction,
    CallForHelpResult,
)
from chargepal_actions.msg import (
    ArriveAtHomeAction,
    ArriveAtHomeResult,
)
from chargepal_actions.msg import (
    PickUpCartAction,
    PickUpCartResult,
)
from chargepal_actions.msg import (
    PlaceCartAction,
    PlaceCartResult,
)
from chargepal_actions.msg import (
    BatteryCommunicationAction,
    BatteryCommunicationResult,
    BatteryCommunicationFeedback,
)
from chargepal_actions.msg import (
    PushRdbcToLdbAction,
    PushRdbcToLdbResult,
    PushRdbcToLdbFeedback,
)
from chargepal_actions.msg import (
    PullRdbToRdbcAction,
    PullRdbToRdbcResult,
    PullRdbToRdbcFeedback,
)


class ChargePal_Actions:
    def __init__(self):
        self.sim_flag = rospy.get_param("/sim_flag")
        self.rdb_path = rospy.get_param("/rdb_path")
        self.rdb_copy_path = rospy.get_param("/rdbc_path")
        self.battery_communication = rospy.get_param("/battery_communication")
        self.arrive_at_station_enabled = rospy.get_param("/arrive_at_station_enabled")
        self.pickup_cart_enabled = rospy.get_param("/pickup_cart_enabled")
        self.drop_cart_enabled = rospy.get_param("/drop_cart_enabled")

        self.recovery_email_account = rospy.get_param("/recovery_email_account")
        self.recovery_email_acc_passwd = rospy.get_param("/recovery_email_acc_passwd")
        self.recovery_email_recipients = rospy.get_param("/recovery_email_recipients")
        if self.recovery_email_account == "":
            self.recovery_enabled = False
        else:
            self.recovery_enabled = rospy.get_param("/recovery_enabled")

        self._as_arrive_at_station = actionlib.SimpleActionServer(
            "arrive_at_station",
            ArriveAtStationAction,
            self.arrive_at_station_callback,
            False,
        )
        self._as_arrive_at_station.start()
        self._result_arrive_at_station = ArriveAtStationResult()

        self._as_arrive_at_home = actionlib.SimpleActionServer(
            "arrive_at_home", ArriveAtHomeAction, self.arrive_at_home_callback, False
        )
        self._as_arrive_at_home.start()
        self._result_arrive_at_home = ArriveAtHomeResult()

        self._as_pick_up_cart = actionlib.SimpleActionServer(
            "pick_up_cart", PickUpCartAction, self.pick_up_cart_callback, False
        )
        self._as_pick_up_cart.start()
        self._result_pick_up_cart = PickUpCartResult()

        self._as_place_cart = actionlib.SimpleActionServer(
            "place_cart", PlaceCartAction, self.place_cart_callback, False
        )
        self._as_place_cart.start()
        self._result_place_cart = PlaceCartResult()

        self._as_call_for_help = actionlib.SimpleActionServer(
            "call_for_help", CallForHelpAction, self.call_for_help_callback, False
        )
        self._as_call_for_help.start()
        self._result_call_for_help = CallForHelpResult()

        self._as_battery_communication = actionlib.SimpleActionServer(
            "battery_communication",
            BatteryCommunicationAction,
            self.battery_communication_callback,
            False,
        )
        self._as_battery_communication.start()
        self._result_battery_communication = BatteryCommunicationResult()
        self._feedback_battery_communication = BatteryCommunicationFeedback()

        self._as_push_rdbc_to_ldb = actionlib.SimpleActionServer(
            "db/push_rdbc_to_ldb",
            PushRdbcToLdbAction,
            self.push_rdbc_to_ldb_callback,
            False,
        )
        self._as_push_rdbc_to_ldb.start()
        self._result_push_rdbc_to_ldb = PushRdbcToLdbResult()
        self._feedback_push_rdbc_to_ldb = PushRdbcToLdbFeedback()

        self._as_pull_rdb_to_rdbc = actionlib.SimpleActionServer(
            "db/pull_rdb_to_rdbc",
            PullRdbToRdbcAction,
            self.pull_rdb_to_rdbc_callback,
            False,
        )
        self._as_pull_rdb_to_rdbc.start()
        self._result_pull_rdb_to_rdbc = PullRdbToRdbcResult()

        self.another_action_running = False

    def place_cart_callback(self, goal: PlaceCartAction):
        """
        Callback function for the 'place_cart' action.

        Args:
            goal (PlaceCartAction): The goal object containing the action parameters.

        Returns:
            None
        """

        if self.sim_flag or not self.drop_cart_enabled:
            self.another_action_running = True
            rospy.sleep(6)
            self._result_place_cart.action_status = "Finished"
            self._result_place_cart.cart_placed = True

        else:

            headers, api_url = mir_rest_api_initialise()
            mission_group_guid = get_mir_missions_group_guid(headers, api_url)
            mission_guid = get_mir_mission_guid(
                headers, api_url, mission_group_guid, "Place_Cart"
            )

            try:
                if clear_mir_error():
                    if not self.another_action_running:
                        self.another_action_running = True

                        id_ = post_mission(
                            headers,
                            api_url,
                            mission_guid,
                            "Executing PlaceCart",
                        )

                        done_state = False
                        state_text = " "
                        while not done_state or state_text == "Executing":
                            done_state, state_text = get_state(headers, api_url, id_)
                            if state_text == "EmergencyStop" or state_text == "Error":
                                break

                        if state_text == "Ready":
                            self._result_place_cart.action_status = "Finished"
                            self._result_place_cart.cart_placed = True

                        else:
                            enter_log_file(
                                f"    chargepal_actions_ERROR:Unable to finish PlaceCart action. The action state is {state_text}."
                            )

                            if reset_mission_queue():
                                enter_log_file(f"Resetting Mir mission queue.")

                            else:
                                enter_log_file(
                                    f"    chargepal_actions_ERROR: Unable to reset Mir mission queue after 10 tries!"
                                )
                            self._result_place_cart.action_status = state_text
                            self._result_place_cart.cart_placed = False

                    else:
                        self._result_place_cart.action_status = "AnotherActionRunning"
                        self._result_place_cart.cart_placed = False
                        enter_log_file(
                            f"    chargepal_actions_ERROR:Unable to perform PlaceCart action. Another action running."
                        )

                else:
                    self._result_place_cart.action_status = "FailedClearMirError"
                    self._result_place_cart.cart_placed = False
                    enter_log_file(
                        f"    chargepal_actions_ERROR:Unable to perform PlaceCart action. Failed clearing MiR error."
                    )

            except Exception as e:
                self._result_place_cart.action_status = f"Exception:{e}"
                self._result_place_cart.cart_placed = False
                enter_log_file(
                    f"    chargepal_actions_ERROR: PlaceCart action reached exception - {e}."
                )

        self.another_action_running = False
        self._as_place_cart.set_succeeded(self._result_place_cart)

    def pick_up_cart_callback(self, goal: PickUpCartAction):
        """
        Callback function for the 'pick_up_cart' action.

        Args:
            goal (PickUpCartAction): The goal object containing the desired action parameters.

        Returns:
            None
        """
        if self.sim_flag or not self.pickup_cart_enabled:
            self.another_action_running = True
            rospy.sleep(6)

            self._result_pick_up_cart.action_status = "Finished"
            self._result_pick_up_cart.cart_picked = True

        else:

            headers, api_url = mir_rest_api_initialise()
            mission_group_guid = get_mir_missions_group_guid(headers, api_url)
            mission_guid = get_mir_mission_guid(
                headers, api_url, mission_group_guid, "Pickup_Cart"
            )

            try:
                if clear_mir_error():
                    if not self.another_action_running:
                        self.another_action_running = True

                        id_ = post_mission(
                            headers, api_url, mission_guid, "Executing PickupCart"
                        )

                        done_state = False
                        state_text = " "
                        while not done_state or state_text == "Executing":

                            done_state, state_text = get_state(headers, api_url, id_)
                            if state_text == "EmergencyStop" or state_text == "Error":
                                break

                        if state_text == "Ready":

                            self._result_pick_up_cart.action_status = "Finished"
                            self._result_pick_up_cart.cart_picked = True

                        else:
                            enter_log_file(
                                f"    chargepal_actions_ERROR:Unable to perform PickupCart action. The action state is {state_text}."
                            )

                            if reset_mission_queue():
                                enter_log_file(f"Resetting Mir mission queue.")

                            else:
                                enter_log_file(
                                    f"    chargepal_actions_ERROR: Unable to reset Mir mission queue after 10 tries!"
                                )

                            self._result_pick_up_cart.action_status = state_text
                            self._result_pick_up_cart.cart_picked = False

                    else:
                        self._result_pick_up_cart.action_status = (
                            "AnotherActionRunning"
                        )
                        self._result_pick_up_cart.cart_picked = False
                        enter_log_file(
                            f"    chargepal_actions_ERROR:Unable to perform PickupCart action. Another action running."
                        )
                else:
                    self._result_pick_up_cart.action_status = "FailedClearMirError"
                    self._result_pick_up_cart.cart_placed = False
                    enter_log_file(
                        f"    chargepal_actions_ERROR:Unable to perform PickupCart action. Failed clearing MiR error."
                    )

            except Exception as e:
                self._result_place_cart.action_status = f"Exception:{e}"
                self._result_place_cart.cart_placed = False
                enter_log_file(
                    f"    chargepal_actions_ERROR: PickupCart action reached exception - {e}."
                )

        self.another_action_running = False
        self._as_pick_up_cart.set_succeeded(self._result_pick_up_cart)

    def arrive_at_station_callback(self, goal: ArriveAtStationAction):
        """
        Callback function for the 'pick_up_cart' action.

        Args:
            goal (ArriveAtStationAction): The goal object containing the desired action parameters.

        Returns:
            None
        """

        target_station = goal.target_station

        if self.sim_flag or not self.arrive_at_station_enabled:
            self.another_action_running = True
            rospy.sleep(6)
            self._result_arrive_at_station.action_status = "Finished"
            self._result_arrive_at_station.station_reached = True
            self._result_arrive_at_station.current_station = target_station

        else:

            headers, api_url = mir_rest_api_initialise()
            mission_group_guid = get_mir_missions_group_guid(headers, api_url)
            mission_guid = get_mir_mission_guid(
                headers, api_url, mission_group_guid, "MT_" + goal.target_station
            )

            try:

                if clear_mir_error():
                    if not self.another_action_running:
                        self.another_action_running = True

                        id_ = post_mission(
                            headers,
                            api_url,
                            mission_guid,
                            f"Executing MT_ {goal.target_station}",
                        )

                        done_state = False
                        state_text = " "
                        while not done_state or state_text == "Executing":
                            done_state, state_text = get_state(headers, api_url, id_)
                            if state_text == "EmergencyStop" or state_text == "Error":
                                break

                        if state_text == "Ready":

                            self._result_arrive_at_station.action_status = "Finished"
                            self._result_arrive_at_station.station_reached = True
                            self._result_arrive_at_station.current_station = (
                                target_station
                            )

                        else:
                            enter_log_file(
                                f"    chargepal_actions_ERROR: Unable to perform MT_{goal.target_station}. The action state is {state_text}."
                            )
                            if reset_mission_queue():
                                enter_log_file(f"Resetting Mir mission queue.")

                            else:
                                enter_log_file(
                                    f"    chargepal_actions_ERROR: Unable to reset Mir mission queue after 10 tries!"
                                )

                            self._result_arrive_at_station.action_status = state_text
                            self._result_arrive_at_station.station_reached = False
                            self._result_arrive_at_station.current_station = ""

                    else:
                        self._result_arrive_at_station.action_status = (
                            "AnotherActionRunning"
                        )
                        self._result_arrive_at_station.station_reached = False
                        self._result_arrive_at_station.current_station = ""
                        enter_log_file(
                            f"    chargepal_actions_ERROR: Unable to perform MT_{goal.target_station}. Another action running."
                        )
                else:
                    self._result_arrive_at_station.action_status = (
                        "FailedClearMirError"
                    )
                    self._result_arrive_at_station.station_reached = False
                    self._result_arrive_at_station.current_station = ""
                    enter_log_file(
                        f"    chargepal_actions_ERROR: Unable to perform MT_{goal.target_station}. Failed clearing MiR error."
                    )

            except Exception as e:
                self._result_arrive_at_station.action_status = f"Exception:{e}"
                self._result_arrive_at_station.station_reached = False
                self._result_arrive_at_station.current_station = ""
                enter_log_file(
                    f"    chargepal_actions_ERROR: MT_{goal.target_station} action reached exception - {e}."
                )

        self.another_action_running = False
        self._as_arrive_at_station.set_succeeded(self._result_arrive_at_station)

    def arrive_at_home_callback(self, goal: ArriveAtHomeAction):
        """
        Action callback for arrive at station

        :param goal: goal to be performed - contains the robot base station to be moved to
        """
        if self.sim_flag or not self.arrive_at_station_enabled:

            self.another_action_running = True
            rospy.sleep(6)
            self._result_arrive_at_home.action_status = "Finished"
            self._result_arrive_at_home.station_reached = True
            self._result_arrive_at_home.current_station = goal.target_station

        else:

            headers, api_url = mir_rest_api_initialise()
            mission_group_guid = get_mir_missions_group_guid(headers, api_url)
            mission_guid = get_mir_mission_guid(
                headers, api_url, mission_group_guid, "MT_" + goal.target_station
            )

            try:
                if clear_mir_error():
                    if not self.another_action_running:
                        self.another_action_running = True
                        id_ = post_mission(
                            headers, api_url, mission_guid, f"MT_{goal.target_station}"
                        )

                        done_state = False
                        state_text = " "

                        while not done_state or state_text == "Executing":

                            done_state, state_text = get_state(headers, api_url, id_)
                            if state_text == "EmergencyStop" or state_text == "Error":
                                break

                        if state_text == "Ready":

                            self._result_arrive_at_home.action_status = "Finished"
                            self._result_arrive_at_home.station_reached = True
                            self._result_arrive_at_home.current_station = (
                                goal.target_station
                            )

                        else:
                            enter_log_file(
                                f"    chargepal_actions_ERROR: Unable to perform MT_{goal.target_station}. The action state is {state_text}."
                            )
                            if reset_mission_queue():
                                enter_log_file(f"Resetting Mir mission queue.")

                            else:
                                enter_log_file(
                                    f"    chargepal_actions_ERROR: Unable to reset Mir mission queue after 10 tries!"
                                )

                            self._result_arrive_at_home.action_status = state_text
                            self._result_arrive_at_home.station_reached = False
                            self._result_arrive_at_home.current_station = ""

                    else:
                        self._result_arrive_at_home.action_status = (
                            "AnotherActionRunning"
                        )
                        self._result_arrive_at_home.station_reached = False
                        self._result_arrive_at_home.current_station = ""
                        enter_log_file(
                            f"    chargepal_actions_ERROR: Unable to perform MT_{goal.target_station}. Another action running."
                        )
                else:
                    self._result_arrive_at_home.action_status = "FailedClearMirError"
                    self._result_arrive_at_home.station_reached = False
                    self._result_arrive_at_home.current_station = ""
                    enter_log_file(
                        f"    chargepal_actions_ERROR: Unable to perform MT_{goal.target_station}. Failed clearing MiR error."
                    )

            except Exception as e:
                self._result_arrive_at_home.action_status = f"Exception:{e}"
                self._result_arrive_at_home.station_reached = False
                self._result_arrive_at_home.current_station = ""
                enter_log_file(
                    f"    chargepal_actions_ERROR: MT_{goal.target_station} action reached exception - {e}."
                )

        self.another_action_running = False
        self._as_arrive_at_home.set_succeeded(self._result_arrive_at_home)

    def call_for_help_callback(self, goal: CallForHelpAction):
        """
        Action callback for call help

        :param goal: goal to be performed - empty
        """
        if not self.sim_flag and self.recovery_enabled:
            self.another_action_running = True
            recipients = self.recovery_email_recipients
            sender = self.recovery_email_account
            password = self.recovery_email_acc_passwd

            em = EmailMessage()
            em["From"] = sender
            em["To"] = ", ".join(recipients)
            em["Subject"] = "Message from Chargepal system!"
            em.set_content(
                """"ChargePal CALL FOR HELP'!! This is to notify that the ChargePal system has a problem! Please rectify."""
            )
            context = ssl.create_default_context()
            try:
                with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
                    smtp.login(sender, password)
                    smtp.sendmail(sender, recipients, em.as_string())

            except Exception as e:
                enter_log_file(
                    f"    chargepal_actions_ERROR: Call for help action reached exception - {e}."
                )
        else:
            rospy.loginfo(
                "Calling for help! Manually reset the action / release emergency switch."
            )
            enter_log_file(
                "Calling for help to manually reset the action / release emergency switch."
            )
        self.another_action_running = False
        self._as_call_for_help.set_succeeded()

    def battery_communication_callback(self, goal: BatteryCommunicationAction):
        response_grpc = None
        status = ""
        cart_name = goal.cart_name
        enter_log_file(f"Requested {goal.request_name} for {cart_name}.")

        if self.battery_communication:
            client_instance = Grpc_Client()
            response_grpc, status = client_instance.battery_communication(
                cart_name, goal.request_name,goal.station_name
            )

            while response_grpc == None or status == "":
                self._feedback_battery_communication.status = "Waiting for response"
                self._as_battery_communication.publish_feedback(
                    self._feedback_battery_communication
                )

            self._result_battery_communication.success = response_grpc.success
            self._feedback_battery_communication.status = status
        
        else:
            rospy.sleep(6)
            self._result_battery_communication.success = True
            self._feedback_battery_communication.status = "Finished"

        self._as_battery_communication.publish_feedback(
            self._feedback_battery_communication
        )
        self._as_battery_communication.set_succeeded(self._result_battery_communication)

    def push_rdbc_to_ldb_callback(self, goal):
        ldb_push_list = []
        packaged_strings = []

        with sqlite3.connect(self.rdb_copy_path) as rdb_copy_connection:
            rdb_copy_cursor = rdb_copy_connection.cursor()

            # Get all table names
            rdb_copy_cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            )
            tables = rdb_copy_cursor.fetchall()

            for table in tables:
                table = table[0]

                # Select rows where ldb_push is False
                query = f"SELECT * FROM {table} WHERE ldb_push = 0"
                rdb_copy_cursor.execute(query)
                rows = rdb_copy_cursor.fetchall()

                # Get column names for the current table
                exclude_columns = ["update_timestamp", "ldb_push"]
                all_columns = fetch_column_names(rdb_copy_cursor, table)
                selected_columns = [
                    col for col in all_columns if col not in exclude_columns
                ]

                table_data = {}
                for row in rows:
                    row_dict = {
                        col: row[idx] for idx, col in enumerate(selected_columns)
                    }
                    row_name = row_dict.get("name")
                    if row_name:
                        table_data[row_name] = row_dict

                if table_data:
                    packaged_strings.append(str({table: table_data}))

                    for row_name in table_data.keys():
                        ldb_push_list.append((table, row_name))

        client_instance = Grpc_Client()
        response_grpc, status = client_instance.push_to_ldb(packaged_strings)

        self._feedback_push_rdbc_to_ldb.ongoing_state = "Processing completed"
        self._as_push_rdbc_to_ldb.publish_feedback(self._feedback_push_rdbc_to_ldb)

        self._result_push_rdbc_to_ldb.push_success = False
        if response_grpc is not None:
            self._result_push_rdbc_to_ldb.push_success = response_grpc.success
            if self._result_push_rdbc_to_ldb.push_success:
                enter_log_file("Pushing data to LDB successful")

                # Update ldb_push to True for all rows in the failure lists
                with sqlite3.connect(self.rdb_copy_path) as rdb_copy_connection:
                    rdb_copy_cursor = rdb_copy_connection.cursor()

                    for table, row_name in ldb_push_list:
                        query = f"UPDATE {table} SET ldb_push = 1 WHERE name = ?"
                        rdb_copy_cursor.execute(query, (row_name,))

                    rdb_copy_connection.commit()

            else:
                enter_log_file(
                    f"chargepal_service_ERROR: Pushing data to LDB unsuccessful. Grpc service response is {response_grpc.success} and connection status is {status}."
                )

        self._as_push_rdbc_to_ldb.set_succeeded(self._result_push_rdbc_to_ldb)

    def pull_rdb_to_rdbc_callback(self, goal: PullRdbToRdbcAction):
        """
        Callback function for the 'pull_rdb_to_rdbc' action.

        This function is responsible for pulling data from the RDB (Robot Database) and inserting it into the RDBC (Robot Database Copy).

        Parameters:
        - goal: The goal object containing the parameters for the action.

        Returns:
        - None
        """

        pull_success = False
        # Open the RDB database in read-only mode
        conn_rdb = sqlite3.connect(f"file:{self.rdb_path}?mode=ro", uri=True)
        cursor_rdb = conn_rdb.cursor()

        # Open the RDBC database
        conn_rdbc = sqlite3.connect(self.rdb_copy_path)
        cursor_rdbc = conn_rdbc.cursor()

        clear_table_values(goal.parameters, cursor_rdbc)
        conn_rdbc.commit()

        # Start a transaction to ensure a consistent snapshot of rdb.db
        conn_rdb.execute("BEGIN TRANSACTION;")

        for param in goal.parameters:
            additional_columns = ["ldb_push", "update_timestamp"]

            table_name = param.table
            row_names = param.row_names
            row_names = [item for item in row_names if item!='none']

            rdbc_columns = fetch_column_names(cursor_rdbc, table_name)
            rdb_rdbc_common_columns = [
                col for col in rdbc_columns if col not in additional_columns
            ]
            common_columns_str = ", ".join(rdb_rdbc_common_columns)

            for row_name in row_names:
                row_rdb = extract_row_contents(
                    cursor_rdb, table_name, common_columns_str, row_name
                )

                try:

                    insert_statement = f"INSERT INTO {table_name} ({', '.join(rdb_rdbc_common_columns)}, ldb_push, update_timestamp) VALUES ({', '.join(['?' for _ in rdb_rdbc_common_columns])}, ?, ?)"
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    values = list(row_rdb) + [True, current_time]
                    cursor_rdbc.execute(insert_statement, values)
                    pull_success = True
                except sqlite3.Error as e:
                    print(f"Error occurred: {e}")

        conn_rdbc.commit()
        conn_rdb.close()
        conn_rdbc.close()
        self._result_pull_rdb_to_rdbc.pull_success = pull_success
        self._as_pull_rdb_to_rdbc.set_succeeded(self._result_pull_rdb_to_rdbc)


if __name__ == "__main__":
    rospy.init_node("chargepal_action_server")

    server = ChargePal_Actions()
    rospy.spin()
