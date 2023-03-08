from redfish.rest.v1 import ServerDownOrUnreachableError
import redfish
import sys


login_host = "https://x.x.x.x:8443"
login_account = "myuser"
login_password = "mypassword"

REDFISH_OBJ = redfish.redfish_client(base_url=login_host, username=login_account, password=login_password, default_prefix='/redfish/v1/')

# Login
try:
    REDFISH_OBJ.login(auth="session")
except ServerDownOrUnreachableError as excp:
    sys.stderr.write("Error: server not reachable or does not support RedFish.\n")
    sys.exit()

# Get the system information /redfish/v1/Systems/1/SmartStorage/
# /redfish/v1/Systems/1/Processors/
# /redfish/v1/Systems/1/Memory/proc1dimm1/
response = REDFISH_OBJ.get(sys.argv[1])
# Print the system information
print(response.dict)

# Logout
REDFISH_OBJ.logout()
