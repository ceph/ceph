from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError

from ceph_node_proxy.redfish_client import RedFishClient
from ceph_node_proxy.util import get_logger, normalize_dict, to_snake_case


@dataclass
class ComponentUpdateSpec:
    collection: str
    path: str
    fields: List[str]
    attribute: Optional[str] = None


class EndpointMgr:
    """Manages Redfish root endpoints (Systems, Chassis, etc.) discovered from the service root."""

    NAME: str = "EndpointMgr"

    def __init__(
        self, client: RedFishClient, prefix: str = RedFishClient.PREFIX
    ) -> None:
        self.log = get_logger(f"{__name__}:{EndpointMgr.NAME}")
        self.prefix: str = prefix
        self.client: RedFishClient = client
        self._endpoints: Dict[str, "Endpoint"] = {}
        self._session_url: str = ""

    def __getitem__(self, index: str) -> Endpoint:
        if index not in self._endpoints:
            raise KeyError(
                f"'{index}' is not a valid endpoint. Available: {list(self._endpoints.keys())}"
            )
        return self._endpoints[index]

    def get(self, name: str, default: Any = None) -> Any:
        return self._endpoints.get(name, default)

    def list_endpoints(self) -> List[str]:
        return list(self._endpoints.keys())

    @property
    def session(self) -> str:
        return self._session_url

    def init(self) -> None:
        error_msg: str = "Can't discover entrypoint(s)"
        try:
            _, _data, _ = self.client.query(endpoint=self.prefix)
            json_data: Dict[str, Any] = json.loads(_data)

            for k, v in json_data.items():
                if isinstance(v, dict) and "@odata.id" in v:
                    name: str = to_snake_case(k)
                    url: str = v["@odata.id"]
                    self.log.info(f"entrypoint found: {name} = {url}")
                    self._endpoints[name] = Endpoint(url, self.client)

            try:
                self._session_url = json_data["Links"]["Sessions"]["@odata.id"]
            except (KeyError, TypeError):
                self.log.warning("Session URL not found in root response")
                self._session_url = ""

        except (URLError, KeyError, json.JSONDecodeError) as e:
            msg = f"{error_msg}: {e}"
            self.log.error(msg)
            raise RuntimeError(msg) from e


class Endpoint:
    """Single Redfish resource or collection; supports lazy child resolution and member listing."""

    NAME: str = "Endpoint"

    def __init__(self, url: str, client: RedFishClient) -> None:
        self.log = get_logger(f"{__name__}:{Endpoint.NAME}")
        self.url: str = url
        self.client: RedFishClient = client
        self._children: Dict[str, "Endpoint"] = {}
        self.data: Dict[str, Any] = self.get_data()
        self.id: str = ""
        self.members_names: List[str] = []

        if self.has_members:
            self.members_names = self.get_members_names()

        if self.data:
            try:
                self.id = self.data["Id"]
            except KeyError:
                self.id = self.data["@odata.id"].split("/")[-1]
        else:
            self.log.warning(f"No data could be loaded for {self.url}")

    def __getitem__(self, key: str) -> "Endpoint":
        if not isinstance(key, str) or not key or "/" in key:
            raise KeyError(key)

        if key not in self._children:
            child_url: str = f'{self.url.rstrip("/")}/{key}'
            self._children[key] = Endpoint(child_url, self.client)

        return self._children[key]

    def list_children(self) -> List[str]:
        return list(self._children.keys())

    def query(self, url: str) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        try:
            self.log.debug(f"Querying {url}")
            _, _data, _ = self.client.query(endpoint=url)
            if not _data:
                self.log.warning(f"Empty response from {url}")
            else:
                data = json.loads(_data)
        except KeyError as e:
            self.log.error(f"KeyError while querying {url}: {e}")
        except HTTPError as e:
            self.log.error(f"HTTP error while querying {url} - {e.code} - {e.reason}")
        except json.JSONDecodeError as e:
            self.log.error(f"JSON decode error while querying {url}: {e}")
        except Exception as e:
            self.log.error(
                f"Unexpected error while querying {url}: {type(e).__name__}: {e}"
            )
        return data

    def get_data(self) -> Dict[str, Any]:
        return self.query(self.url)

    def get_members_names(self) -> List[str]:
        result: List[str] = []
        if self.has_members:
            for member in self.data["Members"]:
                name: str = member["@odata.id"].split("/")[-1]
                result.append(name)
        return result

    def get_name(self, endpoint: str) -> str:
        return endpoint.split("/")[-1]

    def get_members_endpoints(self) -> Dict[str, str]:
        members: Dict[str, str] = {}
        self.log.debug(
            f"get_members_endpoints called on {self.url}, has_members={self.has_members}"
        )

        if self.has_members:
            url_parts = self.url.split("/redfish/v1/")
            if len(url_parts) > 1:
                base_path = "/redfish/v1/" + url_parts[1].split("/")[0]
            else:
                base_path = None

            for member in self.data["Members"]:
                name = self.get_name(member["@odata.id"])
                endpoint_url = member["@odata.id"]
                self.log.debug(f"Found member: {name} -> {endpoint_url}")

                if base_path and not endpoint_url.startswith(base_path):
                    self.log.warning(
                        f"Member endpoint {endpoint_url} does not match base path {base_path} "
                        f"from {self.url}. Skipping this member."
                    )
                    continue

                members[name] = endpoint_url
        else:
            if self.data:
                name = self.get_name(self.url)
                members[name] = self.url
                self.log.warning(
                    f"No Members array, using endpoint itself: {name} -> {self.url}"
                )
            else:
                self.log.debug(f"Endpoint {self.url} has no data and no Members array")

        return members

    def get_members_data(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        self.log.debug(
            f"get_members_data called on {self.url}, has_members={self.has_members}"
        )

        if self.has_members:
            self.log.debug(
                f'Endpoint {self.url} has Members array: {self.data.get("Members", [])}'
            )
            members_endpoints = self.get_members_endpoints()

            if not members_endpoints:
                self.log.warning(
                    f"Endpoint {self.url} has Members array but no valid members after filtering. "
                    f"Using endpoint itself as singleton resource."
                )
                if self.data:
                    name = self.get_name(self.url)
                    result[name] = self.data
            else:
                for member, endpoint_url in members_endpoints.items():
                    self.log.debug(
                        f"Fetching data for member: {member} at {endpoint_url}"
                    )
                    result[member] = self.query(endpoint_url)
        else:
            self.log.debug(
                f"Endpoint {self.url} has no Members array, returning own data"
            )
            if self.data:
                name = self.get_name(self.url)
                result[name] = self.data
            else:
                self.log.warning(f"Endpoint {self.url} has no members and empty data")

        return result

    @property
    def has_members(self) -> bool:
        return bool(
            self.data
            and "Members" in self.data
            and isinstance(self.data["Members"], list)
        )


def build_data(
    data: Dict[str, Any],
    fields: List[str],
    log: Any,
    attribute: Optional[str] = None,
) -> Dict[str, Dict[str, Dict]]:
    result: Dict[str, Dict[str, Optional[Dict]]] = dict()

    def process_data(m_id: str, flds: List[str], d: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for field in flds:
            try:
                out[to_snake_case(field)] = d[field]
            except KeyError:
                log.debug(f"Could not find field: {field} in data: {d}")
                out[to_snake_case(field)] = None
        return out

    try:
        if attribute is not None:
            data_items = data[attribute]
        else:
            data_items = [{"MemberId": k, **v} for k, v in data.items()]
        log.debug(f"build_data: data_items count={len(data_items)}")
        for d in data_items:
            member_id = d.get("MemberId")
            result[member_id] = {}
            result[member_id] = process_data(member_id, fields, d)
    except (KeyError, TypeError, AttributeError) as e:
        log.error(f"Can't build data: {e}")
        raise
    return normalize_dict(result)


def _resolve_path(endpoint: Endpoint, path: str) -> Endpoint:
    """Resolve an endpoint by traversing path segments (example: 'PowerSubsystem/PowerSupplies')."""
    parts = [p for p in path.split("/") if p]
    current = endpoint
    for part in parts:
        current = current[part]
    return current


def get_component_data(
    endpoints: EndpointMgr,
    collection: str,
    path: str,
    fields: List[str],
    log: Any,
    attribute: Optional[str] = None,
) -> Dict[str, Any]:
    """Build component data from Redfish endpoints. Returns dict sys_id -> member_id -> data."""
    members: List[str] = endpoints[collection].get_members_names()
    result: Dict[str, Any] = {}
    if not members:
        ep = _resolve_path(endpoints[collection], path)
        data = ep.get_members_data()
        result = build_data(data=data, fields=fields, log=log, attribute=attribute)
    else:
        for member in members:
            try:
                ep = _resolve_path(endpoints[collection][member], path)
                if attribute is None:
                    data = ep.get_members_data()
                else:
                    data = ep.data
                result[member] = build_data(
                    data=data, fields=fields, log=log, attribute=attribute
                )
            except HTTPError as e:
                log.error(f"Error while updating {path}: {e}")
                continue
    return result


def update_component(
    endpoints: EndpointMgr,
    collection: str,
    component: str,
    path: str,
    fields: List[str],
    _sys: Dict[str, Any],
    log: Any,
    attribute: Optional[str] = None,
) -> None:
    """Update _sys[component] from Redfish endpoints using the given spec.
    path can be a single segment ('Memory') or multiple ('PowerSubsystem/PowerSupplies').
    """
    _sys[component] = get_component_data(
        endpoints, collection, path, fields, log, attribute=attribute
    )
