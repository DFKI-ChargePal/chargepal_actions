The chargepal_actions package contains all the action servers affliated with the robot. The table below shows the available actions and its description.

| Name | Description |
| ------ | ------ |
|arrive_at_station|move the robot from a source station to target station|
|arrive_at_home|dock robot to its charger|
|pick_up_charger|Pick up the cart|
|place_charger|Place cart|
|connect_plug_to_car|Plugin to the adpater station|
|disconnect_plug_from_car|Plugout from adapter station|
|plugin|Plugin to charge cart|
|plugout|Plugout from charging the cart|
|call_for_help|Call technician for assitance|

**MiR based actions**
- Missions like MT_<station_name>, Pickup_Cart, Place_Cart are created inside Chargepal missions list inside MiR. 
- MiR RestAPI call is then used to call a respective mission.
- ROS actions are wrapped around these RestAPI calls.


