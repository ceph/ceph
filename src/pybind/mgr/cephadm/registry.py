import json
from typing import List, Dict, Tuple, TYPE_CHECKING, Set

import requests
from pkg_resources import parse_version
from requests import Response, RequestException

from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


class Registry:

    def __init__(self, url: str):
        self._url: str = url

    @property
    def api_domain(self) -> str:
        if self._url == 'docker.io':
            return 'registry-1.docker.io'
        return self._url

    def get_token(self, response: Response) -> str:
        realm, params = self.parse_www_authenticate(response.headers['Www-Authenticate'])
        r = requests.get(realm, params=params)
        r.raise_for_status()
        ret = r.json()
        if 'access_token' in ret:
            return ret['access_token']
        if 'token' in ret:
            return ret['token']
        raise ValueError(f'Unknown token reply {ret}')

    def parse_www_authenticate(self, text: str) -> Tuple[str, Dict[str, str]]:
        # 'Www-Authenticate': 'Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:ceph/ceph:pull"'

        r: Dict[str, str] = {}
        for token in text.split(','):
            key, value = token.split('=', 1)
            r[key] = value.strip('"')
        realm = r.pop('Bearer realm')
        return realm, r

    def get_tags(self, image) -> List[str]:
        url = f'https://{self.api_domain}/v2/{image}/tags/list'
        r = requests.get(url, headers={'Accept': 'application/json'})

        token = self.get_token(r)
        r = requests.get(url, headers={'Accept': 'application/json', 'Authorization': f'Bearer {token}'})

        r.raise_for_status()
        return r.json()['tags']

def get_latest_container_image_tag(mgr: "CephadmOrchestrator") -> str:

    from mgr_module import MonCommandFailed

    try:
        ret, out, err = mgr.check_mon_command({
            'prefix': 'mgr versions',
        })
        j = json.loads(out)
    except (MonCommandFailed, ValueError) as e:
        raise OrchestratorError(f'Unable to get list of versions: `ceph mgr versions` failed: {e}')

    versions = [v.split(' ')[2] for v in j.keys() if v.startswith('ceph version ')]
    major: List[int] = [int(v.split('.')[0]) for v in versions if '.' in v]

    if not major:
        raise OrchestratorError('Unable to get list of versions: `ceph mgr versions` failed')

    latest_major = str(max(major))

    reg_name, image = mgr.container_image_base.split('/', 1)

    try:
        raw_tags = Registry(reg_name).get_tags(image)
    except RequestException as e:
        raise OrchestratorError(f'Unable to get list of versions for {mgr.container_image_base}: {e}')

    tags = set()
    for tag in raw_tags:
        if tag.startswith('v'):
            tag = tag[1:]
        if not tag.startswith(f'{latest_major}.'):
            continue
        tags.add(tag)

    if not tags:
        raise OrchestratorError(f'Unable to get list of versions for {mgr.container_image_base}: empty list\n{raw_tags}')

    latest = sorted(tags, key=parse_version, reverse=True)

    return latest[0]






