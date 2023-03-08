from redfish_system import RedfishSystem
import time

host = "https://95.128.41.119:8443"
username = "redo"
password = "cmVkbzp5RyEyJktFZ0JSdzg"

system = RedfishSystem(host, username, password)
system.start_update_loop()
time.sleep(20)
print(system.get_status())
system.stop_update_loop()
