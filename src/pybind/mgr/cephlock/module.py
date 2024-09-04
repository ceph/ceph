import cherrypy
import threading
import datetime
import json
from typing import Optional
from mgr_module import CLICommand, CLIReadCommand, MgrModule, Option, OptionValue, PG_STATES


_LOCK_PREFIX = "lock/"

class FleetLockAPI:
    def __init__(self, module):
        self.module = module

    @cherrypy.expose
    @cherrypy.tools.json_in(
        force=False, content_type=["application/x-www-form-urlencoded", ""]
    )
    @cherrypy.tools.json_out()
    def pre_reboot(self, **kwargs):
        # try:
        #     print(kwargs)
        #     print(cherrypy.request.header_list)
        #     print(cherrypy.request.json)
        # except:
        #     print("kaputt")
        if not cherrypy.request.method == "POST":
            cherrypy.response.status = 405
            return {
                "kind": "wrong_http_method",
                "value": "Unsupported HTTP method.",
            }
        if not self.validate_protocol_header:
            cherrypy.response.status = 400
            return {
                "kind": "missing_http_header",
                "value": "Fleetlock Header is missing.",
            }
        if not self.validate_payload(cherrypy.request.json):
            cherrypy.response.status = 400
            return {
                "kind": "malformed_json_body",
                "value": "The request not conform to the Fleetlock Spec.",
            }
        id = cherrypy.request.json["client_params"]["id"]
        group = cherrypy.request.json["client_params"]["group"]
        if not self.check_health():
            cherrypy.response.status = 400
            return {
                "kind": "failed_lock_cluster_not_healthy",
                "value": "Updating ist not possible while cluster unhealthy",
            }
        if not self.check_updates_enabled():
            cherrypy.response.status = 400
            return {
                "kind": "failed_lock_updating_not_enabled",
                "value": "Updating is disabled by configuration",
            }
        if not self.check_time_slots():
            cherrypy.response.status = 400
            return {
                "kind": "failed_lock_not_in_timewindow",
                "value": "Updating is disabled at this time",
            }
        success, message = self.acquire_lock(group=group, id=id)
        if success:
            cherrypy.response.status = 200
        else:
            cherrypy.response.status = 400
        return message

    @cherrypy.expose
    @cherrypy.tools.json_in(
        force=False, content_type=["application/x-www-form-urlencoded", ""]
    )
    @cherrypy.tools.json_out()
    def steady_state(self, **kwargs):
        # try:
        #     print(kwargs)
        #     print(cherrypy.request.header_list)
        #     print(cherrypy.request.json)
        # except:
        #     print("kaputt")
        if not cherrypy.request.method == "POST":
            cherrypy.response.status = 405
            return {
                "kind": "wrong_http_method",
                "value": "Unsupported HTTP method.",
            }
        if not self.validate_protocol_header:
            cherrypy.response.status = 400
            return {
                "kind": "missing_http_header",
                "value": "Fleetlock Header is missing.",
            }
        if not self.validate_payload(cherrypy.request.json):
            cherrypy.response.status = 400
            return {
                "kind": "malformed_json_body",
                "value": "The request not conform to the Fleetlock Spec.",
            }
        id = cherrypy.request.json["client_params"]["id"]
        group = cherrypy.request.json["client_params"]["group"]
        success, message = self.release_lock(group=group, id=id)
        if success:
            cherrypy.response.status = 200
        else:
            cherrypy.response.status = 400
        return message

    def validate_protocol_header(self):
        protocol_header = cherrypy.request.headers.get("fleet-lock-protocol")
        return protocol_header == "true"

    def validate_payload(self, data):
        if not isinstance(data, dict):
            return False, "Invalid JSON format"
        if "client_params" not in data:
            return False, "Missing 'client_params' in JSON"
        client_params = data["client_params"]
        if not isinstance(client_params, dict):
            return False, "'client_params' must be an object"
        if "group" not in client_params or "id" not in client_params:
            return False, "'client_params' must include 'group' and 'id'"
        return True

    def check_health(self):
        if self.module.get_health()["status"] == "HEALTH_OK":
            return True
        return False

    def check_updates_enabled(self):
        result = self.module.read_kv("enableUpdates")
        return result == "True"

    def check_time_slots(self):
        try:
            after = datetime.time.fromisoformat(self.module.read_kv("UpdateOnlyAfter"))
            before = datetime.time.fromisoformat(self.module.read_kv("UpdateOnlyBefore"))
        except ValueError:
            # log parsing error
            return False
        current_time = datetime.datetime.now().time()
        if after < before:
            return after <= current_time <= before
        else:
            return current_time >= after or current_time <= before

    def acquire_lock(self, group, id):
        lookup_key = _LOCK_PREFIX+group
        with self.module.mutex:
            existing_id = self.module.get_store(key=lookup_key)
            if existing_id == id:
                return True, {
                    "kind": "lock_granted",
                    "value": "Ready to reboot!",
                }
            elif existing_id:
                return False, {
                    "kind": "failed_lock_semaphore_full",
                    "value": "semaphore currently full, all slots are locked already",
                }
            self.module.set_store(key=lookup_key, val=id)
            return True, {"kind": "lock_granted", "value": "Ready to reboot!"}

    def release_lock(self, group, id):
        lookup_key = _LOCK_PREFIX+group
        with self.module.mutex:
            existing_id = self.module.get_store(key=lookup_key)
            if existing_id == id:
                self.module.set_store(key=lookup_key, val=None)
                return True, {
                    "kind": "lock_release",
                    "value": "Lock succsefully released",
                }
            elif existing_id:
                return False, {
                    "kind": "failed_relase_lock_not_held",
                    "value": f"Can't relase a lock thats owned by {existing_id}",
                }
            return True, {"kind": "lock_not_held", "value": "Ready to lock!"}


class Module(MgrModule):

    @CLIReadCommand("cephlock config-dump")
    def info(self) -> tuple[int, str, str]:
        return (
            0,
            f'Updates Enabled: {self.read_kv("enableUpdates")}\n'
            f'After: {self.read_kv("UpdateOnlyAfter")}\n'
            f'Before: {self.read_kv("UpdateOnlyBefore")}\n'
            f'Port: {self.read_kv("port")}',
            "",
        )

    @CLICommand("cephlock setup")
    def cli_set_config(self, after: str = "invalid",before: str = "invalid", port: int = -1, enabled: Optional[bool] = None) -> tuple[int, str, str]:
        cmd: dict[str, str] = {}
        if not port == -1:
            cmd["port"] = str(port)
        if enabled is not None:
            cmd["enableUpdates"] = str(enabled)
        if not after == "invalid":
            try:
                cmd["UpdateOnlyAfter"] = datetime.time.fromisoformat(after).strftime("%H:%M:%S")
            except ValueError:
                return (1,"",f"Could not parse {after=}   try %H:%M:%S")
        if not before == "invalid":
            try:
                cmd["UpdateOnlyBefore"] = datetime.time.fromisoformat(before).strftime("%H:%M:%S")
            except ValueError:
                return (1,"",f"Could not parse {before=}   try %H:%M:%S")
        for key,value in cmd.items():
            self.write_kv(key, value)
        return self.info()       

    @CLIReadCommand("cephlock status")
    def status(self) -> tuple[int, str, str]:

        result = self.get_store_prefix(_LOCK_PREFIX)
        out = "\n".join([f"\t{key}:{value}" for key,value in result.items()])
        return (
            0,
            f'Updates Enabled: {self.read_kv("enableUpdates")}\n'
            f'After: {self.read_kv("UpdateOnlyAfter")}\n'
            f'Before: {self.read_kv("UpdateOnlyBefore")}\n'
            f'Locks: \n {out}',
            "",
        )


    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.mutex = threading.Lock()
        self.set_defaults()

    def serve(self):
        cherrypy.config.update(
            {
                "server.socket_host": "0.0.0.0",
                "server.socket_port": int(self.read_kv("port")),
            }
        )

        api = FleetLockAPI(self)
        cherrypy.tree.mount(api, "/v1/")

        self.server = cherrypy.engine
        self.server.start()

    def write_kv(self, key: str, value: str) -> None:
        self.set_store(key=key, val=value)

    def read_kv(self, key: str):
        return self.get_store(key=key)

    def get_health(self) -> dict:
        return json.loads(self.get("health")["json"])

    def set_defaults(self):
        with self.mutex:
            if not self.read_kv("port"):
                self.write_kv("port","8080")
            if not self.read_kv("enableUpdates"):
                self.write_kv("enableUpdates", "True")
            if not self.read_kv("UpdateOnlyAfter"):
                after = datetime.time(0, 0, 0).strftime("%H:%M:%S")
                self.write_kv("UpdateOnlyAfter", after)
            if not self.read_kv("UpdateOnlyBefore"):
                before = datetime.time(4, 0, 0).strftime("%H:%M:%S")
                self.write_kv("UpdateOnlyBefore", before)
        return

    def shutdown(self):
        if self.server:
            self.server.exit()


if __name__ == "__main__":
    run = Module()
    run.serve()
