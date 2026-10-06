import cherrypy
import io
import zlib
from typing import Callable, Dict


MAX_DECOMPRESSED_BODY_SIZE = 100 * 1024 * 1024  # 100 MiB


class DecompressedBodyTooLarge(Exception):
    pass


def _decompress_gzip(data: bytes, max_size: int = MAX_DECOMPRESSED_BODY_SIZE) -> bytes:
    """Decompress one gzip stream without allowing unbounded output."""
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    decompressed = decompressor.decompress(data, max_size + 1)

    if len(decompressed) > max_size or decompressor.unconsumed_tail:
        raise DecompressedBodyTooLarge()

    # Agents send a single complete gzip member. Reject truncated streams and
    # trailing/concatenated members rather than silently accepting extra data.
    if not decompressor.eof or decompressor.unused_data:
        raise zlib.error('invalid or truncated gzip stream')

    return decompressed


class CompressionDecoderTool:
    """
    CherryPy tool that transparently decompresses incoming request bodies
    based on the Content-Encoding header.
    Supports: gzip
    """
    decompressors: Dict[str, Callable[[bytes], bytes]] = {
        "gzip": _decompress_gzip,
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
            except DecompressedBodyTooLarge:
                cherrypy.log(
                    f"[compression_in] {encoding} request exceeds "
                    f"{MAX_DECOMPRESSED_BODY_SIZE} byte decompressed limit",
                    severity=30,
                )
                raise cherrypy.HTTPError(413, "Decompressed request body too large")
            except Exception as e:
                cherrypy.log(f"[compression_in] Failed to decompress {encoding}: {e}", severity=40)
                raise cherrypy.HTTPError(400, f"Invalid {encoding} request body")
        elif encoding and encoding != 'identity':
            supported_encodings = ', '.join(list(self.decompressors.keys()) + ['identity'])
            cherrypy.log(f"[compression_in] Unsupported Content-Encoding: {encoding} (supported: {supported_encodings})", severity=30)  # WARNING
            raise cherrypy.HTTPError(415,
                                     f"Unsupported Content-Encoding: {encoding}",
                                     headers=[("Accept-Encoding", supported_encodings)])


# Register the tool
cherrypy.tools.compression_in = cherrypy.Tool('before_handler', CompressionDecoderTool())
