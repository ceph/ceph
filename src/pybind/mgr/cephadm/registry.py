import shlex
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
        """
        Parse Bearer authentication parameters from a WWW-Authenticate header.

        Some registries (e.g. IBM Container Registry) return headers with
        spaces after commas:
            Bearer realm=https://cp.icr.io/oauth/token, service=registry, scope=...
        shlex.split() handles quoted strings natively and treats commas and
        spaces as delimiters, so both quoted and unquoted values work without
        any custom regex.
        """
        r: Dict[str, str] = {}

        # Strip the scheme token ("Bearer ") before parsing key=value pairs.
        bearer_prefix = 'Bearer '
        if text.startswith(bearer_prefix):
            text = text[len(bearer_prefix):]

        # shlex splits on whitespace and respects quoted strings.
        # Replace commas with spaces so they act as plain delimiters, but
        # only outside quoted strings — shlex handles this correctly.
        lex = shlex.shlex(text, posix=True)
        lex.whitespace = ', '
        lex.whitespace_split = True
        for token in lex:
            if '=' in token:
                key, _, value = token.partition('=')
                r[key.strip()] = value

        if 'realm' not in r:
            raise ValueError(f'No realm found in WWW-Authenticate header: {text}')

        realm = r.pop('realm')
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
