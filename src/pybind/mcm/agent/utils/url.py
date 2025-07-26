from urllib.parse import urljoin

def join_urls(base_url: str, relative_url: str) -> str:
    return urljoin(base_url, relative_url)