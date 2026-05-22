"""
Tests for fullchain PEM support in cephadm certmgr (Ceph issue #75710).

Covers:
  - ssl_cert_utils.split_fullchain_pem  — core parser
  - ssl_cert_utils.is_fullchain_pem / contains_private_key  — detectors
  - ssl_cert_utils.get_certificate_info  — must tolerate fullchain input
  - mgr_util.parse_combined_pem_file  — must handle PKCS#1 and EC key types
  - CertMgr.save_cert_key_from_pem  — high-level ingest helper
  - module.cert_store_set_pair  — CLI entry point
  - CephadmService._get_certificates_from_spec  — service-spec entry point
"""

import pytest
from unittest import mock

# Import the wait() helper used throughout cephadm tests to unwrap OrchResult
from .fixtures import wait

# ---------------------------------------------------------------------------
# Test fixtures — real RSA/EC cert+key material generated with cryptography lib
# ---------------------------------------------------------------------------

# Leaf certificate (signed by INTERMEDIATE_CA_CERT)
LEAF_CERT = """-----BEGIN CERTIFICATE-----
MIIC9TCCAd2gAwIBAgIUelIETaYTD1cwcjFDr7HcOpvYxvQwDQYJKoZIhvcNAQEL
BQAwHzEdMBsGA1UEAwwUVGVzdCBJbnRlcm1lZGlhdGUgQ0EwHhcNMjYwNDI0MTMx
NzE4WhcNMjcwNDI0MTMxNzE4WjAbMRkwFwYDVQQDDBB0ZXN0LmV4YW1wbGUuY29t
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAm+oARuOxuNzvRf0YGU3Y
Eo7WZOLrFDWrSF10qQXG8P9hvPmXMKluplpO+UqXwfuNqD4QQ1+SLVpWbnBTCIRD
fMssuEm0/qhPa1xlL0/s1MN0rKA7R6JsEtY3MG/S1l9pHWDABxkQ89OlU1DE2WmX
qaKRKytfeFFlEUPZy+x89QxRc1RHFLsVDyejMRYNFW5UkJA9VGF34K39QzBg1pN7
mq5bs9zgDg3X9YnIWxBMLibNLT/b9j6s4KO0MeW0yjwvnwqCFi0Q7/nwGSvufsDY
7O0wHiY24u5lYEh82fsKr3/Uwe/1i6gzh5fW1kKr1cGM+7PNEpiLnx0eIydTbfAu
HQIDAQABoy0wKzAbBgNVHREEFDASghB0ZXN0LmV4YW1wbGUuY29tMAwGA1UdEwEB
/wQCMAAwDQYJKoZIhvcNAQELBQADggEBAHp1L6hfRz6hzhTqqJ1paJnhjpIa2dj+
H4ded/mchtXGoneACTfvU2tRXt7KMlKTHXXZvlWOkbgF78RR/6p59915g3M/D+Xq
uMapLbBMQRy6C6GpsbtuV7Cfn51TnU7nNBbBnpY4PXLOZpIp53rZhpaPvSNbRgLs
7Eq9+c0pYTMq3wwTppFGinXgJCP728AUZgZZv1ur+VFYAyCSL6WC3P7hulxiEcRA
tu6/A2wwN/ymaA625GmTOU02ctNorA72gX+NQT8ttnbIMKtduqpiL8JTIbIwYf70
xKMtXkEAQYAQdpcjvVY83X1r4LPl84xTJi76AJO6W4Wl8qhCwaE87HI=
-----END CERTIFICATE-----"""

INTERMEDIATE_CA_CERT = """-----BEGIN CERTIFICATE-----
MIIC2jCCAcKgAwIBAgIUJvwLJWw/yp0k1yvQ8nTS6dZE28MwDQYJKoZIhvcNAQEL
BQAwFzEVMBMGA1UEAwwMVGVzdCBSb290IENBMB4XDTI2MDQyNDEzMTcxOFoXDTM2
MDQyMTEzMTcxOFowHzEdMBsGA1UEAwwUVGVzdCBJbnRlcm1lZGlhdGUgQ0EwggEi
MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC+eEwIRFpfQU8yYyeD4RrmTZOu
qbpnzpJEaCndrM/fiRwud3wMrGR+rETnleUxz9M5b5rVH0E7bViYX5DuStum1cA7
uqpXGQxavk2DQW78jLJ59A5gbznyOIKgYAFD5GWVr726oiGX20DOWo79lJ52vqe7
RORxdZxOeeZ0Vrt5rciB9xy6LN1jfh0V24NrsJIjQNGPZ3DxPvf1yCNV2G+WTa8f
m3rU1h+xl1/BgtFWE1dMi1p1r9/HEGkcAuaYqt4lM0gR72ayaBY01jh2NlQbGZPS
yCsygWsTocj8jB5oJ4T3Ph1ZRc8AXhjkA/Zy4wQt0EILtR+lT/SW07SouAuLAgMB
AAGjFjAUMBIGA1UdEwEB/wQIMAYBAf8CAQAwDQYJKoZIhvcNAQELBQADggEBAFWv
0UTxcWMRMvX0cDsU7vutv3NOKWylmmyB+6sTZtoqgjlx2E7DewJXzQckUGUUPD0+
qX+gsoNoc2QIN0VzdFxW8UoFPSa+262UzrmP0HIK6wiGKRjatLFMIl2iPEg+rFOJ
PY/tj6zTTy5hjajsOdXzvZTLzanz54FPTWBUlS7t+j6krWwGYSf60JdmRIVYGEKb
bJSTBNe/w3USXDHw62pr6I+mNlNCPZMTSaQy36np+8/TnqnZDVhi8F9rafoB/Bku
coA2Se6MxKnHiYL7YoePQgtREHt8/4/iVzeN8N9sT0svy2KBK4mm64JHv+lewINH
TwsaLHX6SSnHQvjnt5w=
-----END CERTIFICATE-----"""

ROOT_CA_CERT = """-----BEGIN CERTIFICATE-----
MIICzzCCAbegAwIBAgIURDrX7VBwPac2fBTbyhYZISOGc3AwDQYJKoZIhvcNAQEL
BQAwFzEVMBMGA1UEAwwMVGVzdCBSb290IENBMB4XDTI2MDQyNDEzMTcxOFoXDTM2
MDQyMTEzMTcxOFowFzEVMBMGA1UEAwwMVGVzdCBSb290IENBMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtBXy9pj7QNhtYXOQhrZwPLH8NsweGNdvgnxb
J+Pcl9N9aQG1B/CiEf1ARSmzRrtplsbrDkwDhaPJccuU58boMGFSwtpJx1VHDK9O
+tXj8SprHKNKJ4MjqT/RPcJa4rjiDm+PomQvpI6QL9VNrjyKLeGtv2I7k86NxDTk
cM7y4+MjpUEB9E2T1yBFWh7rBv+JOK2GeXoUYUrp7ib2xh60gHFtyRr4XgfurAvk
Nm44GyqFgeaZClgv9Mxr3XfFxiRXlinxIt1GPewQn+iee1qQucYNStLPAtTQrRBA
1FqzE0zdC0AUAQFvNpD3TqzkadOZLiEr4yXrBNyJBFpFXBP3GQIDAQABoxMwETAP
BgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAAQTGQ82cKn585rBh3
ZP0+NuW0bPK0kvqFgxODiBJmR08yg808lTzIEIPClXahXfyIt6SoFvRc82KJhyyL
8bshUpSkOXFcZ9ikHhjbBHKkwIuNilvvHxxMqsvBgBK6NCb6ZwbBF+czhBDD4WY9
u0KXDGZBwfEYb3VcJxt+2/+CLFEBKqKzyuKbWTB+kcMUQeO7/0Rh+F/7mXah9RzU
dmLlNdSl1R0i3NvB3Oktdn99uILdJQVyfMzgLCQHe/I214Hji12xBO8Qx3KJreKY
wCBT6sxFcAa/xRSK8wZlai+tNqxpWaxllzbcs5xgnHPRUWFxztBDmdHKIrlHdwaa
J7sp
-----END CERTIFICATE-----"""

# PKCS#1 key matching LEAF_CERT (RSA PRIVATE KEY)
LEAF_KEY_PKCS1 = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAm+oARuOxuNzvRf0YGU3YEo7WZOLrFDWrSF10qQXG8P9hvPmX
MKluplpO+UqXwfuNqD4QQ1+SLVpWbnBTCIRDfMssuEm0/qhPa1xlL0/s1MN0rKA7
R6JsEtY3MG/S1l9pHWDABxkQ89OlU1DE2WmXqaKRKytfeFFlEUPZy+x89QxRc1RH
FLsVDyejMRYNFW5UkJA9VGF34K39QzBg1pN7mq5bs9zgDg3X9YnIWxBMLibNLT/b
9j6s4KO0MeW0yjwvnwqCFi0Q7/nwGSvufsDY7O0wHiY24u5lYEh82fsKr3/Uwe/1
i6gzh5fW1kKr1cGM+7PNEpiLnx0eIydTbfAuHQIDAQABAoIBACWASulCHo7nqzCj
H+8EK37JNvWko+TkRMq+2c7GyjNQxd45jV1Bv5DJI3owMCDTI98t8GT4IZWBh00A
fORNRdtIFj1MUzm2W4Xn/xl6aK1DRva3gpKoFUURm8wtdWGlKMgNa5q5c3umMjuA
L5zYmkdNyAuBXvD2aPAWaRY0z1h0KiwP7tb13yXVEvWTf78X94xX2FeXt08k4o2A
FfV13zMT2rtjAbRIGcb9hn1a00Hj+VGSHqzsnFzQjSEDnor9b8J0AFokbVCggIxS
MWn9yX8Pw+oGb15f67ibk3qcdU/PrjEeD2p+UjbxmDi/xv5Ei9nq0g7Xe0H8TYWS
Ar9ITfkCgYEAyqm9/oMOJ/NFQLiw1vR+axnI/BHOslD2YAMMbwHi2nrQVLxVRtRl
/PdoPZHOLkQZjGzTG1GSpMqsTeleeiHwDnYSSgRog5vNhVe2hbViAI6BVC1oeTO5
z6vXfnTyLSgGl6npv/co8DW8fLuFlbU6xYx6DAZRtskY4gKN9Kxz7hkCgYEAxPKV
gGBkDunzLCHBBls2R2anSGo4EjrnNRQ3XFBOSJuGL74OMEedA5qqmj6stEed1TpZ
k6tH/Z7BfqzZRB+tF5/s9JnM1+tuOVTJaZyjKg9eDRx/wvUcdVAnFSss7Z50KNEp
6LSpSAiDwM10gCGAENhmKuyK4GyK+wasy+f8eKUCgYEAgt/O9BcKAz6UUFF00bue
D+fc5PtS8dBa6nHNi7o6F0EMXEwq/cyX+B6FUI1iCnqrzQVR5uhsvMKtNrsn6dMU
xSH2eZoTLDpnJF8aXYpeuWFNn2CbgPmoWrXsOZun8QVSDIsLio4//6+UAzDN+XnJ
dF1dS3qhNlrzGLDxnznmu7ECgYBihTdkUNO84itTGE+G7nnoneFwyDHkbLLcSpCn
DUUb7TDjER/n5usUsnpFTrT2Oh0qXVYSGMyagqAozi6hdXcRKl9OvOFL4enxpAhd
XI4CrE1QIcGHtTXXOZFTdZW0CF/zSy67yiQkdJ30BNMSha+avaXeMxYFzkZ37I7k
MdiKQQKBgBmmtM84YJammDT5g3wdKg0V9UDX7W6Ed6UPjDfpfQqR5mEEqGhkPagz
tje/YcFlxpm7zlZoAyinmnxJ0ufnHBArsga34Ho6plwF9xXaZEtw/PXxJjt5jFBV
Yyr8xGhXYE6fG72U5+q3eax+wclimULwqO1RtRIJqEKYSIMPkOks
-----END RSA PRIVATE KEY-----"""

# PKCS#8 key matching LEAF_CERT (PRIVATE KEY)
LEAF_KEY_PKCS8 = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCb6gBG47G43O9F
/RgZTdgSjtZk4usUNatIXXSpBcbw/2G8+ZcwqW6mWk75SpfB+42oPhBDX5ItWlZu
cFMIhEN8yyy4SbT+qE9rXGUvT+zUw3SsoDtHomwS1jcwb9LWX2kdYMAHGRDz06VT
UMTZaZepopErK194UWURQ9nL7Hz1DFFzVEcUuxUPJ6MxFg0VblSQkD1UYXfgrf1D
MGDWk3uarluz3OAODdf1ichbEEwuJs0tP9v2Pqzgo7Qx5bTKPC+fCoIWLRDv+fAZ
K+5+wNjs7TAeJjbi7mVgSHzZ+wqvf9TB7/WLqDOHl9bWQqvVwYz7s80SmIufHR4j
J1Nt8C4dAgMBAAECggEAJYBK6UIejuerMKMf7wQrfsk29aSj5OREyr7ZzsbKM1DF
3jmNXUG/kMkjejAwINMj3y3wZPghlYGHTQB85E1F20gWPUxTObZbhef/GXporUNG
9reCkqgVRRGbzC11YaUoyA1rmrlze6YyO4AvnNiaR03IC4Fe8PZo8BZpFjTPWHQq
LA/u1vXfJdUS9ZN/vxf3jFfYV5e3TyTijYAV9XXfMxPau2MBtEgZxv2GfVrTQeP5
UZIerOycXNCNIQOeiv1vwnQAWiRtUKCAjFIxaf3Jfw/D6gZvXl/ruJuTepx1T8+u
MR4Pan5SNvGYOL/G/kSL2erSDtd7QfxNhZICv0hN+QKBgQDKqb3+gw4n80VAuLDW
9H5rGcj8Ec6yUPZgAwxvAeLaetBUvFVG1GX892g9kc4uRBmMbNMbUZKkyqxN6V56
IfAOdhJKBGiDm82FV7aFtWIAjoFULWh5M7nPq9d+dPItKAaXqem/9yjwNbx8u4WV
tTrFjHoMBlG2yRjiAo30rHPuGQKBgQDE8pWAYGQO6fMsIcEGWzZHZqdIajgSOuc1
FDdcUE5Im4Yvvg4wR50DmqqaPqy0R53VOlmTq0f9nsF+rNlEH60Xn+z0mczX6245
VMlpnKMqD14NHH/C9Rx1UCcVKyztnnQo0SnotKlICIPAzXSAIYAQ2GYq7IrgbIr7
BqzL5/x4pQKBgQCC3870FwoDPpRQUXTRu54P59zk+1Lx0Frqcc2LujoXQQxcTCr9
zJf4HoVQjWIKeqvNBVHm6Gy8wq02uyfp0xTFIfZ5mhMsOmckXxpdil65YU2fYJuA
+ahatew5m6fxBVIMiwuKjj//r5QDMM35ecl0XV1LeqE2WvMYsPGfOea7sQKBgGKF
N2RQ07ziK1MYT4bueeid4XDIMeRsstxKkKcNRRvtMOMRH+fm6xSyekVOtPY6HSpd
VhIYzJqCoCjOLqF1dxEqX0684Uvh6fGkCF1cjgKsTVAhwYe1Ndc5kVN1lbQIX/NL
LrvKJCR0nfQE0xKFr5q9pd4zFgXORnfsjuQx2IpBAoGAGaa0zzhglqaYNPmDfB0q
DRX1QNftboR3pQ+MN+l9CpHmYQSoaGQ9qDO2N79hwWXGmbvOVmgDKKeafEnS5+cc
ECuyBrfgejqmXAX3FdpkS3D89fEmO3mMUFVjKvzEaFdgTp8bvZTn6rd5rH7ByWKZ
QvCo7VG1EgmoQphIgw+Q6Sw=
-----END PRIVATE KEY-----"""

# EC key + self-signed EC cert
EC_CERT = """-----BEGIN CERTIFICATE-----
MIIBXzCCAQWgAwIBAgIUZrGXXM3YIc5e/VHPN2dzLnT7y7kwCgYIKoZIzj0EAwIw
GTEXMBUGA1UEAwwOZWMuZXhhbXBsZS5jb20wHhcNMjYwNDI0MTMxNzE4WhcNMjcw
NDI0MTMxNzE4WjAZMRcwFQYDVQQDDA5lYy5leGFtcGxlLmNvbTBZMBMGByqGSM49
AgEGCCqGSM49AwEHA0IABG6etoWfzNYDEr17rWrqtemrULoy7SZXZ1qzuGX04Tke
1u0iGrzCg6wnFmzVo3OSpMy0M1UQT2SmapI2fMYGwwWjKzApMBkGA1UdEQQSMBCC
DmVjLmV4YW1wbGUuY29tMAwGA1UdEwEB/wQCMAAwCgYIKoZIzj0EAwIDSAAwRQIg
XEar4lz6MrOShJ7hFwGJ9GpBBMKZe83ROxttCo/3eVQCIQDq4JZJRxgFx1AAppmb
KF4VF31uzV1GusTQn8J4uQKFhQ==
-----END CERTIFICATE-----"""

EC_KEY = """-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIKY5DCziSptEBpTN2l8vTdspDAW5aNrgARSwSbWaVsH3oAoGCCqGSM49
AwEHoUQDQgAEbp62hZ/M1gMSvXutauq16atQujLtJldnWrO4ZfThOR7W7SIavMKD
rCcWbNWjc5KkzLQzVRBPZKZqkjZ8xgbDBQ==
-----END EC PRIVATE KEY-----"""

# A key that does NOT match LEAF_CERT
UNRELATED_KEY_PKCS1 = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAmnk5pZkG5SMEYxLp58/PJXYlaNiRxP4dWBERqd2hbKkkC2Rv
J7A7HgvsxRl6ILYxxvpRyDJYryGmZo5ahmn/WAJjpq5QtS0fiWJf7ER/O8SrEqSd
5EmKt+nspuYbczF3FsMocAjHkwewvV2rQydwBlU2oxqNVC9gJA2iQXz9kL5SVddR
SVWiOW+P7QFzi3UojMlaoks/rQ/GxvLGqC/6FW6P5q+CB+qYC7/5NLncEYMxFCfx
hmmNjBTrh8FLzozg2b/1/qh/vpnA2APYds12/1uD6Yy7D6Xe7/mmPo9IDxhTO8nv
4fmZ8+db+qifQCW2UKszaP5OUDZb9qaHElNQRwIDAQABAoIBAD/ExIHyorCL3J9D
qxDZrkNiOv5FzSNb3eRBo5+SkTWIu4sCaoqhSz5ToOoa4tpHLFReeM1iz0543zce
FAvCDTmZPucLteCdIJQ36q5f9oBHZ3HaSB36KAxVQq8Bdhp/NJDOfs6FKVIujg0W
coFV8f6DRfteFjLHHfK0OCO9w/Vwn7FOqfB/H/kYuKJiSzxPjbBzm5kTAyNaVnUO
am31UveRnY2h7GQ9W+HaEgVALtRearHx27w9E0cZXxcE3MDCxeutWdj6P6J1ayTn
OFsuPm7AwVrV7M8F1MXJplQyowj7lzXIrmj8Xkur2KM5cu6+EHNTw4zD/ix9KPrn
lrtUi/ECgYEA0HYj4CdpKGA6Wt74HCBBqpV6hk6ZJENeBXc1BmMR66j2Cr7ZyEpQ
bPe/DmQzvoP2fA+c4c4HrIpM1nfFMfLfQ/NOc6koRw7rE0sLx9oOrrYNgtWmQ699
yD4sn+72KS9YjIzdif6lcFKGAWMVl946v4sr4c8QMeze7Hf51wGSUhECgYEAvbNM
0e+WhlPG5u6yoIiZGCkFPXAl59u9bI2fJSZONNUg8J4WIsCqaMfD/+hrUTUj6R5y
Kz2cF2K3r9lfgp1vM7i2v/bPVdjscofevIL2yzqrWrRdi1ZFlbdnQXFbAeAbhYMn
/YCNlRlZBiiZRI4S9akhvxXjDMjl65y9mbiGJNcCgYAVmdhX1t2fsHD7j5U4HOOR
EJI0rppR1qU8fcDB26w0tjpgnKwmhATXdciXbiyqdjQpYNjJ0TacW2xg4OJPLbKS
8PhVJdclndcgFauv1tmkovq9bvQemWW4RTEMlY9ubxiG3+Xo8bkk7XgzVpgbN4zw
4jP23yT3d2sWJ8x8yg3mcQKBgEpVxWeKnBB9Kbb6mFKh1GQMRvphROlLSToNcOxs
yz6WubawK/poRx6SETLpj4wd57mE7bYWCWF8lIA7DNsw+A9rdWlZvLtmKplitpxm
MXFHxzfe770XDGNzn+mcEs50VDSIRZZn1pMzgJgSNqUPi0xqf0fSussaukYOgJmJ
Hj+3AoGBAITKZECIMrNMwgolAd1bE4216ovfcl6vr1LIZ4FIiUpGYM/rD2+P3uuS
wqR6nnE1kmu0yWCnvuEiejms66jXxtbqrEYTd+8Q9ZBePxbq5s3TFnFhi2KJwMak
+SMK1xVR2+lmZ2yi6K6rxBGXz/6WQtZUBomIWhIujcm3ySMtMr0Z
-----END RSA PRIVATE KEY-----"""


# ---------------------------------------------------------------------------
# Convenience helpers that build common multi-block blobs
# ---------------------------------------------------------------------------

def _fullchain_pkcs1() -> str:
    """key(PKCS#1) + leaf + intermediate + root — the exact format from the issue."""
    return "\n".join([LEAF_KEY_PKCS1, LEAF_CERT, INTERMEDIATE_CA_CERT, ROOT_CA_CERT])


def _fullchain_pkcs8() -> str:
    """key(PKCS#8) + leaf + intermediate."""
    return "\n".join([LEAF_KEY_PKCS8, LEAF_CERT, INTERMEDIATE_CA_CERT])


def _fullchain_ec() -> str:
    """EC key (SEC1) + EC self-signed cert."""
    return "\n".join([EC_KEY, EC_CERT])


def _cert_chain_only() -> str:
    """leaf + intermediate + root — no key."""
    return "\n".join([LEAF_CERT, INTERMEDIATE_CA_CERT, ROOT_CA_CERT])


# ===========================================================================
# Tests: ssl_cert_utils — low-level parsing helpers
# ===========================================================================

class TestSplitFullchainPem:
    """Unit tests for :func:`cephadm.ssl_cert_utils.split_fullchain_pem`."""

    def setup_method(self):
        from cephadm.ssl_cert_utils import split_fullchain_pem
        self.split = split_fullchain_pem

    # --- Happy paths --------------------------------------------------------

    def test_pkcs1_key_with_three_cert_chain(self):
        """PKCS#1 key + leaf + 2 CA certs — the exact format from issue #75710."""
        blob = _fullchain_pkcs1()
        cert_chain, key = self.split(blob)

        assert '-----BEGIN RSA PRIVATE KEY-----' in key
        assert '-----END RSA PRIVATE KEY-----' in key
        assert '-----BEGIN CERTIFICATE-----' in cert_chain
        # Leaf cert must appear first
        assert cert_chain.index(LEAF_CERT.strip()) < cert_chain.index(INTERMEDIATE_CA_CERT.strip())
        # No key material in the cert chain
        assert '-----BEGIN RSA PRIVATE KEY-----' not in cert_chain
        assert '-----BEGIN PRIVATE KEY-----' not in cert_chain

    def test_pkcs8_key_with_two_cert_chain(self):
        """PKCS#8 (unencrypted) key + leaf + intermediate."""
        blob = _fullchain_pkcs8()
        cert_chain, key = self.split(blob)

        assert '-----BEGIN PRIVATE KEY-----' in key
        assert '-----END PRIVATE KEY-----' in key
        # Both certs present in chain
        assert LEAF_CERT.strip() in cert_chain
        assert INTERMEDIATE_CA_CERT.strip() in cert_chain

    def test_ec_key_with_self_signed_cert(self):
        """SEC1 EC key + EC self-signed cert."""
        blob = _fullchain_ec()
        cert_chain, key = self.split(blob)

        assert '-----BEGIN EC PRIVATE KEY-----' in key
        assert '-----END EC PRIVATE KEY-----' in key
        assert EC_CERT.strip() in cert_chain

    def test_cert_only_no_key(self):
        """A PEM with only CERTIFICATE blocks (no key) is accepted — key returned empty."""
        blob = _cert_chain_only()
        cert_chain, key = self.split(blob)

        assert key == ''
        assert LEAF_CERT.strip() in cert_chain
        assert INTERMEDIATE_CA_CERT.strip() in cert_chain
        assert ROOT_CA_CERT.strip() in cert_chain

    def test_single_cert_no_key(self):
        """A plain single-cert PEM is accepted without error."""
        cert_chain, key = self.split(LEAF_CERT)
        assert LEAF_CERT.strip() in cert_chain
        assert key == ''

    def test_output_is_normalised_no_trailing_garbage(self):
        """Output should contain only clean PEM blocks, no surrounding whitespace noise."""
        blob = f"\n\n   \n{_fullchain_pkcs1()}\n\n"
        cert_chain, key = self.split(blob)
        # Each returned string should start cleanly
        assert cert_chain.startswith('-----BEGIN CERTIFICATE-----')
        assert key.startswith('-----BEGIN RSA PRIVATE KEY-----')

    # --- Error paths --------------------------------------------------------

    def test_raises_on_empty_input(self):
        from cephadm.ssl_cert_utils import SSLConfigException
        with pytest.raises(SSLConfigException, match='No PEM blocks'):
            self.split('')

    def test_raises_on_no_cert_block(self):
        from cephadm.ssl_cert_utils import SSLConfigException
        with pytest.raises(SSLConfigException, match='no CERTIFICATE blocks'):
            self.split(LEAF_KEY_PKCS1)

    def test_raises_on_duplicate_key_blocks(self):
        from cephadm.ssl_cert_utils import SSLConfigException
        blob = "\n".join([LEAF_KEY_PKCS1, LEAF_KEY_PKCS8, LEAF_CERT])
        with pytest.raises(SSLConfigException, match='2 private key blocks'):
            self.split(blob)

    def test_raises_on_key_cert_mismatch(self):
        """Key that does not match the leaf cert must be rejected."""
        from cephadm.ssl_cert_utils import SSLConfigException
        blob = "\n".join([UNRELATED_KEY_PKCS1, LEAF_CERT])
        with pytest.raises(SSLConfigException, match='does not match'):
            self.split(blob)

    def test_raises_on_plaintext_garbage(self):
        from cephadm.ssl_cert_utils import SSLConfigException
        with pytest.raises(SSLConfigException):
            self.split('this is not a PEM at all')


class TestPemDetectors:
    """Unit tests for the is_fullchain_pem / contains_private_key helpers."""

    def test_is_fullchain_true_for_key_plus_cert(self):
        from cephadm.ssl_cert_utils import is_fullchain_pem
        assert is_fullchain_pem(_fullchain_pkcs1()) is True

    def test_is_fullchain_true_for_multiple_certs(self):
        from cephadm.ssl_cert_utils import is_fullchain_pem
        assert is_fullchain_pem(_cert_chain_only()) is True

    def test_is_fullchain_false_for_single_cert(self):
        from cephadm.ssl_cert_utils import is_fullchain_pem
        assert is_fullchain_pem(LEAF_CERT) is False

    def test_is_fullchain_false_for_single_key(self):
        from cephadm.ssl_cert_utils import is_fullchain_pem
        assert is_fullchain_pem(LEAF_KEY_PKCS1) is False

    def test_contains_private_key_pkcs1(self):
        from cephadm.ssl_cert_utils import contains_private_key
        assert contains_private_key(LEAF_KEY_PKCS1) is True
        assert contains_private_key(_fullchain_pkcs1()) is True

    def test_contains_private_key_pkcs8(self):
        from cephadm.ssl_cert_utils import contains_private_key
        assert contains_private_key(LEAF_KEY_PKCS8) is True

    def test_contains_private_key_ec(self):
        from cephadm.ssl_cert_utils import contains_private_key
        assert contains_private_key(EC_KEY) is True
        assert contains_private_key(_fullchain_ec()) is True

    def test_contains_private_key_false_for_cert_only(self):
        from cephadm.ssl_cert_utils import contains_private_key
        assert contains_private_key(LEAF_CERT) is False
        assert contains_private_key(_cert_chain_only()) is False


class TestGetCertificateInfoFullchain:
    """get_certificate_info must handle fullchain PEM gracefully."""

    def test_fullchain_returns_leaf_cert_info(self):
        from cephadm.ssl_cert_utils import get_certificate_info
        info = get_certificate_info(_cert_chain_only())
        # Should parse the leaf cert (test.example.com), not the root CA
        assert info.get('subject', {}).get('commonName') == 'test.example.com'

    def test_single_cert_unchanged(self):
        from cephadm.ssl_cert_utils import get_certificate_info
        info = get_certificate_info(LEAF_CERT)
        assert info.get('subject', {}).get('commonName') == 'test.example.com'


# ===========================================================================
# Tests: mgr_util.parse_combined_pem_file — PKCS#1 and EC support
# ===========================================================================

class TestParseCombinedPemFile:
    """parse_combined_pem_file must detect RSA PRIVATE KEY and EC PRIVATE KEY blocks."""

    def setup_method(self):
        from mgr_util import parse_combined_pem_file
        self.parse = parse_combined_pem_file

    def test_pkcs8_key_extracted(self):
        blob = LEAF_KEY_PKCS8 + "\n" + LEAF_CERT
        cert, key = self.parse(blob)
        assert cert is not None and '-----BEGIN CERTIFICATE-----' in cert
        assert key is not None and '-----BEGIN PRIVATE KEY-----' in key

    def test_pkcs1_key_extracted(self):
        """Previously broken — only PKCS#8 was supported."""
        blob = LEAF_KEY_PKCS1 + "\n" + LEAF_CERT
        cert, key = self.parse(blob)
        assert cert is not None and '-----BEGIN CERTIFICATE-----' in cert
        assert key is not None and '-----BEGIN RSA PRIVATE KEY-----' in key

    def test_ec_key_extracted(self):
        """EC PRIVATE KEY was not handled before this fix."""
        blob = EC_KEY + "\n" + EC_CERT
        cert, key = self.parse(blob)
        assert cert is not None and '-----BEGIN CERTIFICATE-----' in cert
        assert key is not None and '-----BEGIN EC PRIVATE KEY-----' in key

    def test_cert_only_returns_none_key(self):
        cert, key = self.parse(LEAF_CERT)
        assert cert is not None
        assert key is None

    def test_key_only_returns_none_cert(self):
        cert, key = self.parse(LEAF_KEY_PKCS1)
        assert cert is None
        assert key is not None

    def test_fullchain_returns_first_cert_only(self):
        """parse_combined_pem_file returns only the *first* cert — callers that
        need the full chain should use split_fullchain_pem instead."""
        blob = _cert_chain_only()
        cert, key = self.parse(blob)
        # Only the first CERTIFICATE block is captured
        assert cert is not None
        assert cert.count('-----BEGIN CERTIFICATE-----') == 1


# ===========================================================================
# Tests: CertMgr.save_cert_key_from_pem — high-level ingest method
# ===========================================================================

class TestCertMgrSaveCertKeyFromPem:
    """Tests for the new CertMgr.save_cert_key_from_pem ingest helper."""

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_pkcs1_split_on_ingest(self, _set_store, cephadm_module):
        """Fullchain PEM (PKCS#1 key + chain) is split and both halves stored."""
        cm = cephadm_module.cert_mgr
        blob = _fullchain_pkcs1()

        cert_chain, key = cm.save_cert_key_from_pem(
            'rgw_ssl_cert', 'rgw_ssl_key', blob, service_name='rgw.myzone'
        )

        # cert_chain must not contain any key material
        assert '-----BEGIN RSA PRIVATE KEY-----' not in cert_chain
        assert '-----BEGIN PRIVATE KEY-----' not in cert_chain
        # key must not contain any cert material
        assert '-----BEGIN CERTIFICATE-----' not in key
        # Both halves persisted
        stored_cert = cm.get_cert('rgw_ssl_cert', service_name='rgw.myzone')
        stored_key = cm.get_key('rgw_ssl_key', service_name='rgw.myzone')
        assert stored_cert is not None and '-----BEGIN CERTIFICATE-----' in stored_cert
        assert stored_key is not None and '-----BEGIN RSA PRIVATE KEY-----' in stored_key

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_pkcs8_split_on_ingest(self, _set_store, cephadm_module):
        cm = cephadm_module.cert_mgr
        blob = _fullchain_pkcs8()
        cert_chain, key = cm.save_cert_key_from_pem(
            'grafana_ssl_cert', 'grafana_ssl_key', blob, host='node1'
        )
        assert '-----BEGIN CERTIFICATE-----' in cert_chain
        assert '-----BEGIN PRIVATE KEY-----' in key

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_ec_split_on_ingest(self, _set_store, cephadm_module):
        cm = cephadm_module.cert_mgr
        blob = _fullchain_ec()
        cert_chain, key = cm.save_cert_key_from_pem(
            'mgmt_gateway_ssl_cert', 'mgmt_gateway_ssl_key', blob
        )
        assert EC_CERT.strip() in cert_chain
        assert '-----BEGIN EC PRIVATE KEY-----' in key

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_cert_chain_only_no_key_stored(self, _set_store, cephadm_module):
        """Chain-only PEM (no key block) — cert saved, key store untouched."""
        cm = cephadm_module.cert_mgr
        blob = _cert_chain_only()
        cert_chain, key = cm.save_cert_key_from_pem(
            'nfs_ssl_cert', 'nfs_ssl_key', blob, service_name='nfs.foo'
        )
        assert key == ''
        stored_cert = cm.get_cert('nfs_ssl_cert', service_name='nfs.foo')
        assert stored_cert is not None

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_key_mismatch_raises(self, _set_store, cephadm_module):
        """Mismatched key/cert in a fullchain blob must raise, not silently store garbage."""
        from cephadm.ssl_cert_utils import SSLConfigException
        cm = cephadm_module.cert_mgr
        blob = "\n".join([UNRELATED_KEY_PKCS1, LEAF_CERT])
        with pytest.raises(SSLConfigException, match='does not match'):
            cm.save_cert_key_from_pem('rgw_ssl_cert', 'rgw_ssl_key', blob, service_name='rgw.x')

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_preserves_intermediate_certs_in_chain(self, _set_store, cephadm_module):
        """All intermediate CA certs must be retained in the stored cert chain."""
        cm = cephadm_module.cert_mgr
        blob = _fullchain_pkcs1()   # leaf + intermediate + root
        cert_chain, _ = cm.save_cert_key_from_pem(
            'rgw_ssl_cert', 'rgw_ssl_key', blob, service_name='rgw.full'
        )
        assert LEAF_CERT.strip() in cert_chain
        assert INTERMEDIATE_CA_CERT.strip() in cert_chain
        assert ROOT_CA_CERT.strip() in cert_chain

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_leaf_cert_is_first_in_chain(self, _set_store, cephadm_module):
        """Leaf cert (first CERTIFICATE block) must appear before intermediate certs."""
        cm = cephadm_module.cert_mgr
        blob = _fullchain_pkcs1()
        cert_chain, _ = cm.save_cert_key_from_pem(
            'rgw_ssl_cert', 'rgw_ssl_key', blob, service_name='rgw.order'
        )
        leaf_pos = cert_chain.index('test.example.com') if 'test.example.com' in cert_chain else \
                   cert_chain.index(LEAF_CERT[:40])
        intermediate_pos = cert_chain.index(INTERMEDIATE_CA_CERT[:40])
        assert leaf_pos < intermediate_pos


# ===========================================================================
# Tests: cert_store_set_pair CLI — fullchain auto-detection
# ===========================================================================

class TestCertStoreSetPairFullchain:
    """cert_store_set_pair must auto-split a fullchain PEM passed as 'cert'."""

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_pkcs1_via_cli(self, _set_store, cephadm_module):
        """Full e2e: fullchain blob → cert_store_set_pair → stored separately."""
        blob = _fullchain_pkcs1()
        # cert_store_set_pair validates with verify_tls; mock that check away
        with mock.patch.object(cephadm_module.cert_mgr, 'check_certificate_state') as mock_check:
            from cephadm.cert_mgr import CertInfo
            mock_check.return_value = CertInfo(
                'rgw_ssl_cert', 'rgw.zone1', user_made=True,
                is_valid=True, is_close_to_expiration=False, days_to_expiration=300
            )
            result = cephadm_module.cert_store_set_pair(
                cert=blob,
                key='',           # no separate key — it's embedded in the blob
                consumer='rgw',
                service_name='rgw.zone1',
            )
        # cert_store_set_pair is decorated with @handle_orch_error → unwrap via wait()
        assert wait(cephadm_module, result) == 'Certificate/key pair set correctly'
        stored_cert = cephadm_module.cert_mgr.get_cert('rgw_ssl_cert', service_name='rgw.zone1')
        stored_key = cephadm_module.cert_mgr.get_key('rgw_ssl_key', service_name='rgw.zone1')
        assert stored_cert is not None and '-----BEGIN CERTIFICATE-----' in stored_cert
        assert '-----BEGIN RSA PRIVATE KEY-----' not in stored_cert   # key must NOT be in cert store
        assert stored_key is not None and '-----BEGIN RSA PRIVATE KEY-----' in stored_key

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_rejected_when_separate_key_also_provided(self, _set_store, cephadm_module):
        """Passing a fullchain blob AND a separate --key is an error.

        handle_orch_error re-raises OrchestratorError immediately (it is a
        user-facing validation error, not a deferred async failure), so the
        exception is raised by cert_store_set_pair() itself — no wait() needed.
        """
        from orchestrator import OrchestratorError
        blob = _fullchain_pkcs1()
        with pytest.raises(OrchestratorError, match='fullchain PEM.*embedded private key'):
            cephadm_module.cert_store_set_pair(
                cert=blob,
                key=LEAF_KEY_PKCS8,  # conflicting extra key
                consumer='rgw',
                service_name='rgw.zone1',
            )


# ===========================================================================
# Tests: _get_certificates_from_spec — service spec entry point
# ===========================================================================

class TestGetCertificatesFromSpecFullchain:
    """_get_certificates_from_spec must split fullchain PEM from service specs."""

    def _make_service(self, cephadm_module):
        """Return a minimal *concrete* CephadmService instance.

        CephadmService has ``TYPE`` as an ``@abstractproperty``, so we create a
        minimal subclass here rather than instantiating the base class directly.
        """
        from cephadm.services.cephadmservice import CephadmService

        class _MinimalService(CephadmService):
            @property
            def TYPE(self) -> str:  # type: ignore[override]
                return 'rgw'

        svc = object.__new__(_MinimalService)
        svc.mgr = cephadm_module
        return svc

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_spec_with_fullchain_pkcs1_split_correctly(self, _set_store, cephadm_module):
        """ssl_cert field contains a fullchain blob — key must be extracted."""
        from cephadm.services.cephadmservice import CephadmDaemonDeploySpec
        svc = self._make_service(cephadm_module)

        # Fake spec object with fullchain in ssl_cert and no ssl_key
        spec = mock.MagicMock()
        spec.service_name.return_value = 'rgw.test'
        spec.ssl_cert = _fullchain_pkcs1()
        spec.ssl_key = None

        daemon_spec = mock.MagicMock(spec=CephadmDaemonDeploySpec)
        daemon_spec.host = 'node1'

        result = svc._get_certificates_from_spec(
            svc_spec=spec,
            daemon_spec=daemon_spec,
            cert_attr='ssl_cert',
            key_attr='ssl_key',
            cert_name='rgw_ssl_cert',
            key_name='rgw_ssl_key',
        )

        # Must not return EMPTY_TLS_CREDENTIALS
        assert result.cert and result.key

        stored_cert = cephadm_module.cert_mgr.get_cert('rgw_ssl_cert', service_name='rgw.test')
        stored_key = cephadm_module.cert_mgr.get_key('rgw_ssl_key', service_name='rgw.test')

        assert stored_cert is not None and '-----BEGIN CERTIFICATE-----' in stored_cert
        assert '-----BEGIN RSA PRIVATE KEY-----' not in stored_cert
        assert stored_key is not None and '-----BEGIN RSA PRIVATE KEY-----' in stored_key

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_spec_fullchain_plus_key_field_returns_empty(self, _set_store, cephadm_module):
        """When both fullchain blob AND a separate key field are set, refuse with EMPTY."""
        from cephadm.tlsobject_types import EMPTY_TLS_CREDENTIALS
        svc = self._make_service(cephadm_module)

        spec = mock.MagicMock()
        spec.service_name.return_value = 'rgw.conflict'
        spec.ssl_cert = _fullchain_pkcs1()
        spec.ssl_key = LEAF_KEY_PKCS8    # conflict!

        daemon_spec = mock.MagicMock()
        daemon_spec.host = 'node1'

        result = svc._get_certificates_from_spec(
            svc_spec=spec,
            daemon_spec=daemon_spec,
            cert_attr='ssl_cert',
            key_attr='ssl_key',
            cert_name='rgw_ssl_cert',
            key_name='rgw_ssl_key',
        )
        assert result == EMPTY_TLS_CREDENTIALS

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_spec_plain_cert_plus_key_unchanged(self, _set_store, cephadm_module):
        """Plain (non-fullchain) cert + separate key still works exactly as before."""
        svc = self._make_service(cephadm_module)

        spec = mock.MagicMock()
        spec.service_name.return_value = 'rgw.plain'
        spec.ssl_cert = LEAF_CERT
        spec.ssl_key = LEAF_KEY_PKCS8

        daemon_spec = mock.MagicMock()
        daemon_spec.host = 'node1'

        result = svc._get_certificates_from_spec(
            svc_spec=spec,
            daemon_spec=daemon_spec,
            cert_attr='ssl_cert',
            key_attr='ssl_key',
            cert_name='rgw_ssl_cert',
            key_name='rgw_ssl_key',
        )

        assert result.cert == LEAF_CERT
        assert result.key == LEAF_KEY_PKCS8
