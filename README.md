The chargepal_actions package contains all the action servers affliated with the robot. The table below shows the available actions and its description.

| Name | Description |
| ------ | ------ |
|arrive_at_station|move the robot from a source station to target station|
|arrive_at_home|dock robot to its charger|
|call_for_help|Call technician for assistance|
|pick_up_charger|Pick up the cart|
|place_charger|Place cart|
|free_drive_arm|Set the arm in free-drive mode|
|move_home_arm|Move the robot arm in a known starting position|
|marker_socket_calib_ads|Calibrate relative offset between marker/pattern and socket - adapter station|
|marker_socket_calib_bcs|Calibrate relative offset between marker/pattern and socket - battery charging station|
|plug_in_ads_ac|Plug-in to the adapter station using the AC female plug|
|plug_in_ads_dc|Plug-in to the adapter station using the DC female plug|
|plug_in_bcs_ac|Plug-in to the battery charging station using the AC male plug|
|plug_out_ads_ac|Plug-out from the adapter station using the AC female plug|
|plug_out_ads_dc|Plug-out from the adapter station using the DC female plug|
|plug_out_bcs_ac|Plug-out from the battery charging station using the AC male plug|
|db/push_rdbc_to_ldb|Push robot database copy to local database|
|db/pull_rdb_to_rdbc|Pull certain row values from robot database to robot database copy|


**MiR based actions**
- Missions like MT_<station_name>, Pickup_Cart, Place_Cart are created inside Chargepal missions list inside MiR. 
- MiR RestAPI call is then used to call a respective mission.
- ROS actions are wrapped around these RestAPI calls.
