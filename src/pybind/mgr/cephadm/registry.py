import re
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
        Parse Bearer authentication parameters from WWW-Authenticate header.
        
        This parser is specifically designed for Bearer authentication headers
        used by container registries (Docker Hub, IBM Container Registry, etc.).
        It is NOT a complete RFC 7235 parser.
        
        Supports both quoted and unquoted parameter values:
        - Quoted: realm="https://auth.docker.io/token"
        - Unquoted: realm=https://auth.docker.io/token
        
        Properly handles quoted values containing commas, e.g.:
        - scope="repository:ceph/ceph:pull,push"
        
        Args:
            text: WWW-Authenticate header value
            
        Returns:
            Tuple of (realm, params_dict) where params_dict contains all
            other authentication parameters (service, scope, etc.)
            
        Raises:
            ValueError: If 'realm' parameter is missing
            
        Examples:
            >>> parse_www_authenticate('Bearer realm="https://auth.docker.io/token",service="registry.docker.io"')
            ('https://auth.docker.io/token', {'service': 'registry.docker.io'})
            
            >>> parse_www_authenticate('Bearer realm=https://cp.icr.io/oauth/token, service=registry')
            ('https://cp.icr.io/oauth/token', {'service': 'registry'})
        """
        r: Dict[str, str] = {}
        
        # Remove 'Bearer ' prefix if present
        if text.startswith('Bearer '):
            text = text[7:].strip()
        
        # Use regex to match key=value pairs with optional quotes
        # Pattern explanation:
        # - (\w+): Capture parameter name (word characters)
        # - \s*=\s*: Match = with optional whitespace
        # - (?:"([^"]*)"|([^,\s]+)): Match either:
        #   - "([^"]*)": quoted value (group 2) - captures everything inside quotes
        #   - ([^,\s]+): unquoted value (group 3) - captures until comma or whitespace
        pattern = r'(\w+)\s*=\s*(?:"([^"]*)"|([^,\s]+))'
        
        for match in re.finditer(pattern, text):
            key = match.group(1)
            # Use quoted value (group 2) if present, otherwise unquoted value (group 3)
            value = match.group(2) if match.group(2) is not None else match.group(3)
            r[key] = value
        
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
