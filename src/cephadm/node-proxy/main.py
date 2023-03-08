from redfish_system import RedfishSystem
import time

host = "https://x.x.x.x:8443"
username = "myuser"
password = "mypassword"

system = RedfishSystem(host, username, password)
system.start_update_loop()
time.sleep(20)
print(system.get_status())
system.stop_update_loop()
