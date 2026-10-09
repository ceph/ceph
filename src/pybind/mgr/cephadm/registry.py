import requests
from typing import List, Dict, Tuple
from urllib.request import parse_http_list
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
        """
        Parse a WWW-Authenticate Bearer challenge into (realm, params).

        IBM Container Registry (cp.icr.io) returns headers with spaces after
        commas; parse_http_list handles HTTP comma-list syntax correctly while
        respecting quoted strings (e.g. scope="repo:x:pull,push").
        """
        try:
            scheme, params = text.split(None, 1)
        except ValueError:
            raise ValueError(f'Invalid WWW-Authenticate header: {text}') from None

        if scheme.lower() != 'bearer':
            raise ValueError(f'Unsupported authentication scheme: {scheme}')

        values: Dict[str, str] = {}
        for item in parse_http_list(params):
            key, sep, value = item.partition('=')
            if not sep:
                raise ValueError(f'Invalid auth parameter: {item}')

            # auth-param allows BWS around '=' (RFC 9110 sec. 11.2)
            key, value = key.strip().lower(), value.strip()

            if len(value) >= 2 and value[0] == value[-1] == '"':
                value = value[1:-1]

            values[key] = value

        if 'realm' not in values:
            raise ValueError(f'no realm in WWW-Authenticate header: {text!r}')

        realm = values.pop('realm')
        return realm, values

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
