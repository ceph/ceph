import requests
from typing import List, Dict, Tuple
from requests import Response


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

    def get_tags(self, image: str) -> List[str]:
        tags = []
        headers = {'Accept': 'application/json'}
        url = f'https://{self.api_domain}/v2/{image}/tags/list'
        while True:
            try:
                r = requests.get(url, headers=headers)
            except requests.exceptions.ConnectionError as e:
                msg = f"Cannot get tags from url '{url}': {e}"
                raise ValueError(msg) from e
            if r.status_code == 401:
                if 'Authorization' in headers:
                    raise ValueError('failed authentication')
                token = self.get_token(r)
                headers['Authorization'] = f'Bearer {token}'
                continue
            r.raise_for_status()

            new_tags = r.json()['tags']
            tags.extend(new_tags)

            if 'Link' not in r.headers:
                break

            # strip < > brackets off and prepend the domain
            url = f'https://{self.api_domain}' + r.headers['Link'].split(';')[0][1:-1]
            continue

        return tags
