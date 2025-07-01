import cherrypy
import io
import gzip
from typing import Callable, Dict


class CompressionDecoderTool:
    """
    CherryPy tool that transparently decompresses incoming request bodies
    based on the Content-Encoding header.
    Supports: gzip
    """
    decompressors: Dict[str, Callable[[bytes], bytes]] = {
        "gzip": lambda b: gzip.decompress(b),
    }

    def __call__(self) -> None:
        encoding = cherrypy.request.headers.get('Content-Encoding', '').lower()
        if encoding in self.decompressors:
            remote_ip = cherrypy.request.remote.ip
            cherrypy.log(f"[compression_in] Decompressing {encoding} request from {remote_ip}", severity=10)  # DEBUG
            try:
                raw_body = cherrypy.request.rfile.read()
                original_size = len(raw_body)
                decompressed = self.decompressors[encoding](raw_body)
                decompressed_size = len(decompressed)
                cherrypy.request.body = io.BytesIO(decompressed)
                cherrypy.request.headers['Content-Encoding'] = 'identity'
                cherrypy.log(f"[compression_in] {encoding} decompressed {original_size} → {decompressed_size} bytes", severity=10)  # DEBUG
            except Exception as e:
                cherrypy.log(f"[compression_in] Failed to decompress {encoding}: {e}", severity=40)
                raise cherrypy.HTTPError(400, f"Invalid {encoding} request body")
        elif encoding and encoding != 'identity':
            cherrypy.log(f"[compression_in] Unsupported Content-Encoding: {encoding}", severity=30)
            raise cherrypy.HTTPError(415, f"Unsupported Content-Encoding: {encoding}")


# Register the tool
cherrypy.tools.compression_in = cherrypy.Tool('before_handler', CompressionDecoderTool())
