import base64
import sys

from OpenSSL import crypto, SSL


def main() -> None:
    crt = base64.b64decode(sys.argv[1]).decode("ascii")
    key = base64.b64decode(sys.argv[2]).decode("ascii")

    try:
        _key = crypto.load_privatekey(crypto.FILETYPE_PEM, key)
        _key.check()
    except (ValueError, crypto.Error) as e:
        print('Invalid private key: %s' % str(e))
    try:
        crt_buffer = crt.encode("ascii") if isinstance(crt, str) else crt
        _crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
    except ValueError as e:
        print('Invalid certificate key: %s' % str(e))

    try:
        context = SSL.Context(SSL.TLSv1_METHOD)
        context.use_certificate(_crt)
        context.use_privatekey(_key)
        context.check_privatekey()
    except crypto.Error as e:
        print('Private key and certificate do not match up: %s' % str(e))
    except SSL.Error as e:
        print(f'Invalid cert/key pair: {e}')


if __name__ == "__main__":
    main()
