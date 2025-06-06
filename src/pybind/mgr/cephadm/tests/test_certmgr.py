import unittest
import re
import pytest
import json
from tests import mock

from cephadm.tlsobject_types import Cert, PrivKey, TLSObjectException, TLSObjectProtocol
from cephadm.tlsobject_store import TLSOBJECT_STORE_PREFIX, TLSObjectStore, TLSObjectScope
from cephadm.module import CephadmOrchestrator
from cephadm.cert_mgr import CertInfo, CertMgr

EXPIRED_CERT = """
-----BEGIN CERTIFICATE-----
MIIFZTCCA02gAwIBAgIUBaoYVIPd+asHvOjo8caYaiipytowDQYJKoZIhvcNAQEL
BQAwQjELMAkGA1UEBhMCWFgxFTATBgNVBAcMDERlZmF1bHQgQ2l0eTEcMBoGA1UE
CgwTRGVmYXVsdCBDb21wYW55IEx0ZDAeFw0yMzEyMjQwNzE1NDdaFw0yMzEyMzAw
NzE1NDdaMEIxCzAJBgNVBAYTAlhYMRUwEwYDVQQHDAxEZWZhdWx0IENpdHkxHDAa
BgNVBAoME0RlZmF1bHQgQ29tcGFueSBMdGQwggIiMA0GCSqGSIb3DQEBAQUAA4IC
DwAwggIKAoICAQC6l2Og/7iE5MCSAgRhBE572eHMckaaLSLhkuuuXvCO6BSUfkxq
7z1xSPu4Hj2iSeSLEo+QG1H6Kh1WOT4x604MNTwtVQb32sXqPFhk7KYLKrC4kfIa
GjfNE2YMtxrJerOiYUZuoxSOkEd/iaIKzNLcAh18gzaUl9YgMEx5UG6a5uZaHpic
SrX3g1jqhQb1EvUZId7vbeN0z+eAaw2lxKeHro02ohDHMZofpoa1q8v1saOYQdIv
g5CgOgNsVeAAKDQMFgWVmSoSf9ds7rV2WJ1heiSPvKlv8Y1OMfuPAfhsPEcyfcxA
Bx1osRFYkTm3TV9m8s8uzmQkMJJO4JOuyQNadchZgqglljavQmBQcl3Euo3C6ptQ
E0ZR9TWznJRasJI51P+f/lihZyuKhFv3TpVqkjthmGFp6PF+xbIXDA22MXD7BO1u
8rwDuPnrNorqrGd/xTM8cmo4K5pDFkmvLDqPi+MtCpQ0Sv0Koenhm7vvmJNaHDga
mSkSAxAooF84xJcCthyWu+wkHa/fKavLCMgi6cYRZ6F8LzYIVs0FJx1SgXsgt6lk
qu25MwzxHnjlutm/cEd3NI4p5hDWH1EKiE6xigJokaTdsB9iFdyMzlYe0Pf/QXzT
/zxhEkyl3EwskZF2/AsxOrzu/6gKOCBC+XW5FtEKw2RKuz+b1RfPyoVM4wIDAQAB
o1MwUTAdBgNVHQ4EFgQUF0pnn/69WPD8vEFoK/+ryRnR/3QwHwYDVR0jBBgwFoAU
F0pnn/69WPD8vEFoK/+ryRnR/3QwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0B
AQsFAAOCAgEAFwvA0iHX8sBrF2G/B1fJ7yx9o0ar2sM27TIwlU7hGAFa9db1YSCe
984cgZVbK1B29cP6jslY9Ev6NcF2z5V9312CjXHBcKuRs30Z6kruGrm0/LaNVJQl
VrKh+whshdd1yR0HypCIEBhOh2xTagnCnc2vBAj0XiV9vGTaCyycxlH1YNvpLusP
5H1Iw6rIzJR18i5pFAY6WaqJfbjZo/jSPEewV+HpxGR6UvLOcrBXNiI/7BZllRE5
ro2PxY/cMlyaeT2arJg4Vduebp4Bn9AHaOa04VSKUP/uGJbWKGearIFVcneeEtzK
rRiv1zsBC1wuAq5qfxW8/O4MRIWBpPimmDalePwVU2Ob3ddTv7pCNnEAqSsUdrC8
R7V/0CyemGh5tLeNuVfaz2TOsJIuRZgyxXW1Mk7bgo6whS5w35goDzUSyu3sdLyW
LxpCRzmRFAcSxdEY5FyOC7hbwzbS+onoB5RFfxRSTps0IYYuGl9t2mHTyEQlwWjl
gyWZ5MjyFPHpjar/VnW7fQaKdEzG4PGYEU3H8IwnazdXEF7AbvRXFMt8uk+12wFA
CCycPdcObZZJtfaL0NyGQ/gXP/RptOEo8zfzj5z/1TdHwcQmBW4ctrACbbdPjYJ6
w/0XhqnUQ6AL4Kc3tFaICvFpks1snRXdhbgwAzltREEyAHs050k+1HU=
-----END CERTIFICATE-----
"""


INVALID_CERT = """
-----BEGIN CERTIFICATE-----
INVALIDCERTDATA
-----END CERTIFICATE-----
"""

NON_MATCHING_KEY = """
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC5xpfgFsX7I19H
GW2YE6vz0TNni2dM1ItQoP0WaX55bNEwLsj9hHTZ7vgTH6ZkaNp0U73Mq+0tM8UP
RrNFBKhy5cE/D+l7aV5KUr4mgPK6Tgrgk0iS83nymladgSKRjN75HH8SMg2lLVoi
vfrAAMh58JA2zFUFZaZQnD1eL/+waht9qpCdilsY3MVKuElZ3ndxSaTuISLhPS8G
O7jkCbCThfkrnk5IeCd5trN8ho55Ev5U5AxgbUgHlJxzUr9wLTzKW0x9D5qbLTva
C9VsUN+SdQW01pTs4MLPuKsnjLGaG91sEbZln4Ub7bXvNey9z0heGE/NJX+Q5Ekk
hFV5TLvZAgMBAAECggEACCGMWi871/X3YJn9mdiISSjsLcS7OEwTgOt/fyd7vhCD
7IoY0j6lwqXazzN3ksgRONAzNOTPyyH5XZyD207DmT4XHVbFGFmQbILsmtDSTuTq
IK1WLSBhjHJW4irHerKGcrNdmHC101MYH0lxHATRU8PW/Ay7c1cqVoCZRnHvFgLQ
YZHxhskDnMTaXX0lw+CCq7ajUg2Su2u7tC7LiG/n4cjBNTblB7vmyAiFo1xoYqam
GuwtkLGZW1RxvCi13HGIKAU9VnwKOyzhJp9ZBcx1Xshiaqazwhpf8PhP8mT2kLFg
ti5NVxadbD78VGMC5bfH6lZdm4/MLlaqMejb6QXCRQKBgQDcd72c4FJpXpXWMR6g
ROw60tn6qjSpH0YJ96bf19UGgNcYVUCiZrgG7ENx6SabjUJwqxi3qCxneD+J7caL
Befd2Can4vf6U3o3DV/a86Dz6Qd4n7n6MU39aOg2jsCriknfOUkWfnGgvMaPzduU
O1rFF0xpezIQkU3HjaN4aLGSswKBgQDXt3/EsRIk8xYQvcUTaWAQdaxtRewS9Tc2
m6MdU6der8C6fTydggUBdkURawFehdpNmKiymBJJFniCs/EuGmKKHjupW04Kmwin
isaA+tSwLQ01tL1G7xhydb85sbfBXzel4fztmk2OB+IpB4rvTFlP8t2z/bQQumjN
WPLUwz7NQwKBgFZ4AD5PHQOGvW3Mxh5F6gEIQcY2i4Dpaybtot2YYUyzq6k3hqor
b3IHqEw9DY9kz/IwqPkfVIsgdos6XuyX3GD+Lesa8feUVhLRhA70DuSbOPruapre
S6BgTPNY+ehNzLtoVGomHZrVb2tnaf+xZ+B1Str0Hqaw1ri1rK/FICBRAoGBALbn
T95mhQvvUPZA8ajT4DAUlm7QqqooYPhcXqGvHGqcer2lEpA6fiQPM+Dg6fhLZh4F
IoTLjDWMaAHqsMR2erbBi7S9Rh6X9W6ZrFYQV+ZJTLoM1bAfaosia1Fv7m53Xae5
Rcvw2XFkHc7MJnFgOxoewvyqUNMeO15h3QOpyMYhAoGABm6bQcIdmv3e+GVoraXA
lsmM4/lRi/HmRHGtQ7kjKvT09YBQ3/qm04QwvwQtik7ws7t8VODQSgZC6re0TU7Y
RPw+RGrt0nnmMUP2jJ6SKPCXmw55tW7FcvBJeAM4komEUoLrnKfwkaRy8SKSt8a0
HlBxebJND7cfu20WpwErmhU=
-----END PRIVATE KEY-----
"""

MATCHING_KEY = """
-----BEGIN PRIVATE KEY-----
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
ofQkxELjoeKZpTzmXhngzU5ltvvP
-----END PRIVATE KEY-----
"""

CEPHADM_SELF_GENERATED_CERT_1 = """
-----BEGIN CERTIFICATE-----
MIIFFzCCAv+gAwIBAgIUHOJMW2YGSTs30hRXi8OhP9TFzuUwDQYJKoZIhvcNAQEL
BQAwJjENMAsGA1UECgwEQ2VwaDEVMBMGA1UEAwwMY2VwaGFkbS1yb290MB4XDTI1
MDEyMTE2MDY1NloXDTI4MDEyMTE2MDY1NlowGjEYMBYGA1UEAwwPMTkyLjE2OC4x
MDAuMTAwMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAj9WNrDts3RUu
8ikG2Qdi5YL2IX1P/9PLXIGM6VBK/DVz4Oh13YBAH54gzJGeBjvZxwDpATgUQuuu
AgREE+3uigzfq6sS31Ru+Gx5WU5LhrHHKOGkofD2Xa04mC0nvcXuOWanItfy1Dmc
MKcl9QywIHp+1qQw0ammYjpvqvRgBIZgg2qqa2rpu+zVHC4omJHaNxAruE6wPTpL
RF5oAIXyZ/0+cyC7YgVqxBOaVQZuH/D36aSSXN/S5ZuNiMUo5Y01hfz989T2iVw6
2KROt6FmBG55EAGNYw7MDtjaBBdkG9zuA7TkdqvgEE2ty7p035rllmu6CxuiToHR
C0w20FrYDkudhHaANB2EQspEE2JhiBnHk8OGhm09mtau9IohQCBGxkz9LJ27IeVK
rs9ElECtn9Ql1a5K6MTxH/dCGKAsH7/0Qg7OHH+2pS7TZ1yd9PhGYdR9ybephxth
288bki1cyg5bXTnhUKBSqORV+26b/dWx3dZ9v7N+oRXmoq6FyOxBP/4DWNAFPUax
gmTGrLM4pa/8IlLkDvn4poyQ4aacjk978LVBJoJlFo4r47gEl3I2WNiTU0D05oXP
aje0xXlgYFOfhZXHBfiny9hPJvpfjIF4MoQJJItfrLhrq5RZpY2QTETOj2A2lzki
wlLZ4D7YwQd0p0xuXDbVPmhCsWmF1C8CAwEAAaNJMEcwNwYDVR0RBDAwLoIVY2Vw
aC1ub2RlLTAuY2VwaC1vcmNogg9ncmFmYW5hX3NlcnZlcnOHBMCoZGQwDAYDVR0T
AQH/BAIwADANBgkqhkiG9w0BAQsFAAOCAgEARJgeYvvbuZkf+h6GGXXRVBmdY6FE
Jt+IHjtLJSmJZyqxNatAPXWHTydnwjN45fOMr2DS4r2Ll8LaUGS0fs+DBUJLSQIN
KDzqj1tnSMMZiHrOAO52m81lz6DE1c9BsOfdc82xCxbFaWqU+5KWtacLMSFAB7oV
ZezjA/G4LkUeNszv2YXw65nj7XKZbo+nn7p802aalbTxyiOpsYd9sa0FGhpczfkf
CZDZfTOhWnmn4yxHhcXI4ndIj6L3njki7/fyljROZjPZU4I3cPvPB2ihhm9H4RiC
A4XtWdZZH1uM+saneO1TGKKBYtEfR5YUnatutHhJ4V6jVUGuRiz6FL+a921dk/IU
kyiQL7MfoeVpoTYCsWM38UL/PeeaU1j47wYHBZJm65tjBsFzpqe/ljYiUHVEJdqn
Didgf03tKBS8KEUAT42EheFDnkEOk4uC16CuTmvREXSWHCfesSIdGH8tIlS6OE6Y
pAVcOecURaqpvdNNGD1tg5qKoO6Fj/In9xMfBD4vct70GIK6n1DsvZDGc29D7TBq
cMKK0xnnIgKE1wea9mNEi3CayN2zwNi4zpINVeJDqJG/fRcYCqVkG7iQFObh63wq
R0AaiVaSDXmCtB0pchTNUEOiGXlO0HUrC4YFVbRvzJRT/d3C5wG76QXbLjSfAzeS
3+7M6o3tu4UActk=
-----END CERTIFICATE-----
"""

CEPHADM_SELF_GENERATED_CERT_2 = """
-----BEGIN CERTIFICATE-----
MIIFFzCCAv+gAwIBAgIUX4PkdhdD70oeykAelVSyhL6AdsUwDQYJKoZIhvcNAQEL
BQAwJjENMAsGA1UECgwEQ2VwaDEVMBMGA1UEAwwMY2VwaGFkbS1yb290MB4XDTI1
MDEyNDEyMjEyMloXDTI4MDEyNDEyMjEyMlowGjEYMBYGA1UEAwwPMTkyLjE2OC4x
MDAuMTAyMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAxiIbWtQf48Cd
UuSFA7PWo3aYmHBkfFluxTi9u0flAaTgA2057DZOOxn3xkXATUFpcsKzGdsHZvR1
C0q0qX38Bsp+21Ctei4vsEVyZA86msxd9AvhLQF5VTyyo0Wc7702qb5dOOp9peXk
1OS+FA46Y2+J9HsF2Y5K3OPPeYx+D1hTY8tUZaus1Zpi72w/zq+zdrRa2pB2Hc9O
BSNOg+ZoU8vZm3LyQK7p5bjCa0llfFD5WIRjJsuQZ+s56ELSTg7asqT3VaiuskrI
xe+atQ+lZj/XJw3TLxoWwYAm9mRtbiOlNL2LsyZVKHlz5Dw1y3RXNbnIZWwZusUn
7aRO3kVj8ZIS+Yhz+TcLqP1p3y9mjmzywZWXrgfiRsCCUIduIczqQbCIeev7JX8D
e7QRQYmVGxEJGJZHmiNvaHUvuReHjbSo4OfogH7GZKX6TBc8pJ2Qfz8ciUaUo6Gi
yf6UdkC7KIMvtYR/YtyuJPH89yFpNi8J/DUvSJYUospBUoQE3S6TF6dIN86NJLDX
7ab+/CxZaePn7XLxxwY1uLjrxQ8J5dxVOU5twS70CK6pfOaK5XfPyUT48jXfHmTd
YLwdstMg/eN/xldAawoqWJP+u3jsQ9eNr28lzMw3enufRdaNoHC1C6ox5tqxYht0
AdcsImFW17FjluN3NnM94DBMud4qK30CAwEAAaNJMEcwNwYDVR0RBDAwLoIVY2Vw
aC1ub2RlLTIuY2VwaC1vcmNogg9ncmFmYW5hX3NlcnZlcnOHBMCoZGYwDAYDVR0T
AQH/BAIwADANBgkqhkiG9w0BAQsFAAOCAgEAXMSDznkjm7LvNqDnvOWYOSWd/mOT
X/BkfdpYc9QmEKJzaIuTaWMyMW4SQt0kZSHG7//1JSF0lBEK882QEkdJ7CUachu4
fcGlNI7zcmOYXVGEW5yBxiJFjeGNZNkkL8uE4uOg1rANf1oWalTNlEWWUNDE1eds
sGcpph1AZMvHQsFhC8q9jyqMkUSyzegK6WYkXrxl2v8mYOKGmQudvblc+mztSHQp
0XTWlowL5dzMHDjCdlFJVfbDkk73acwTTwODizWXa0+EU6JgfViMyypypW/C4/Gl
Z4kAzt8PeVkSQCA30s+63ApMAMgpuItyziBORfv3btleX4gC6zaMtGJQLQrZot69
/fCha1jzxsKbEpZ4/7E4GluSRGA3XshPqzPROITYXWjTuR0J1P8v9kzFrlj17bbj
FSY5HAaKptLHZM8h/g+vTqnAFRIrbKoTnOsnzGtZ9UrIT4BxhNZo3m/8skEXbpau
IfADqfsEQkm09WC1hUn+cSp2AIveqkTQnivL+N7vkiv7tFEeACKEYe3rXUuTM2fq
cYVn8aLcRMIDqOR09aOnLDkqfbnoQEeIALnEZP2sn58hmbP7gfjQjRF/+J2evn7l
MqnoatrcAJsFLNw0hvbQNNBDXzjn4tZ0GQondRp0M2Eqd0qOxQYbrfTe8Zj3g49E
YHhohGcnp4nmo1g=
-----END CERTIFICATE-----
"""

CEPHADM_SELF_GENERATED_KEY_4096 = """
-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
"""

CEPHADM_SELF_GENERATED_KEY_2048 = """
-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCswSOXU7cXhku8
5dp8u41UBg5d/3/biWI2nIt7B/XKvZhDS2JlR9sg0/sMW2NxzOOM8o8R1orc8n+F
jJEivYkBJOc2UwlpHsCQ8Y6bHtQdmoLQge0TWovODzW56e6cFbqkAdEuA1z+CaWj
CkJ8hQH0YP4cNwRaqcDfYClH3iidCnPS2egfOeIJiyyAfsFvgnsJPeYrb3ajgRZL
JlNY9tpkKrPym2Gr2fD0U6hbJeDSASs6vhz6l1uoiWKyJ6aNlfhhS90F7WYXYCbY
3zxnkxDUadCbGhp3g2/kdzaPzDkTP4hQ6wWbtTLMZvJo67SkhtOzdA2N+KSDY41D
Gwz2u3WlAgMBAAECgf9AaydkRoSJj+z85fRYGKuavR2KtJRHq30HzJNKJhAy2IAp
0vJ7ltm9AZ3DOO5mCwD2tXxVZZd2LvcPrlsOD3ffzfa4raCZpgVjuGDrKFi1djcq
JtKjPYLkez2hPV8y1Od1VJqYUtAfTSrZPr8V/oMeifjVUYdOICYuOUsEC3EZk8wx
ZxtXLqqp5vlrwP0ZWueMB2GMvZmlysfh0uMIZmRGlQxWPILMZruhH0xwZIMXVnAd
Igs8y+CIsa6fNgWrou1k07XChOLWgo9elzJfBwETSeERVuYNuF5CtTPzWeI9MDrh
6BHWNnb9bZe/yjy8Py1TA4qPamzPGipgSOJKHQECgYEA56sv10J5iHXSwTscbKp8
YbG77pGkk6eWBRhCt8fnng+ShK0Mt2rO2YdvGpsSnQZooSfBsmWgkYoUEg2TPb1w
8N9NU5L8Wqsr4h07QnzPq6ybW7+KNhiCFBjWIzih8cKJJP2mw45KvOof0IQsunXA
EiPfGx4Ezfgwf0/rG6BWgmUCgYEAvuXw18pxHfYJbbR5x9vTIxfxwq8gTJekm0CI
bifo90T8miNgPxlCZRzUc0jYZfqei/UxoKqjwwtkcGN5TyOTBs7FvHpfU0ixPdYN
ud1fXxqk6KjZNqgFYnK4/9WThAfoFSjRfEtegylSJzGyBIg+biJbjDsNdPyvuBcc
eS8AUkECgYBDRttlz9AraMwDgX8Hr5rfZBYHehQpLQnMAPefF1aT+EG8deSzfzkC
wuno+A+3uhy4cCs3+3tdyJN7iqWv0Ev0J2T1WEIgsoTT7VlAPa6xVDbptf4VJ5je
7TeCkve0laHuNBsxvyjFI9iJXVj/7SISIoiv/0+14NV0o2jLZQy6YQKBgQC3u45r
0z++K4D1or+XWX9EhpY839tBfK6Ecr8c7rKt0ysgm63V7VTXBsF/1/vXYzjX0f2a
+sG1RzC7bzJhtgabhcYRWuKGwS8+Kdh6LJPPcFCKUYEGtv6/u1VNft2FNSrtuqSt
cckcile0u4LwE8WqsMzWEjwofdAOacgQ5ujzQQKBgQC0Ui8bB/Vs4+blmz5mjFdh
JllS3dRKx7Re5bY9XdkxlQd56jxyWIIyXDQ7Dk9GXaCjgGy5J7LaLCi2R1DZx28p
JhTdU/3sfz8NwTTFtR/m84rmirdecK9fYsU5wQzaqq/+IvVD6O8/46tK62YsYkQd
IwuZ9Cw+0P6sn81cI8FaeA==
-----END PRIVATE KEY-----
"""

TLSOBJECT_STORE_CERT_PREFIX = f'{TLSOBJECT_STORE_PREFIX}cert.'
TLSOBJECT_STORE_KEY_PREFIX = f'{TLSOBJECT_STORE_PREFIX}key.'


class TestCertMgr(object):

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_tlsobject_store_save_cert(self, _set_store, cephadm_module: CephadmOrchestrator):

        rgw_frontend_rgw_foo_host2_cert = 'fake-rgw-cert'
        nvmeof_client_cert = 'fake-nvmeof-client-cert'
        nvmeof_server_cert = 'fake-nvmeof-server-cert'
        nvmeof_root_ca_cert = 'fake-nvmeof-root-ca-cert'
        grafana_cert_host_1 = 'grafana-cert-host-1'
        grafana_cert_host_2 = 'grafana-cert-host-2'
        cephadm_module.cert_mgr.save_cert('rgw_frontend_ssl_cert', rgw_frontend_rgw_foo_host2_cert, service_name='rgw.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_server_cert', nvmeof_server_cert, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_client_cert', nvmeof_client_cert, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_root_ca_cert', nvmeof_root_ca_cert, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('grafana_cert', grafana_cert_host_1, host='host-1', user_made=True)
        cephadm_module.cert_mgr.save_cert('grafana_cert', grafana_cert_host_2, host='host-2', user_made=True)

        expected_calls = [
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}rgw_frontend_ssl_cert', json.dumps({'rgw.foo': Cert(rgw_frontend_rgw_foo_host2_cert, True).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}nvmeof_server_cert', json.dumps({'nvmeof.foo': Cert(nvmeof_server_cert, True).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}nvmeof_client_cert', json.dumps({'nvmeof.foo': Cert(nvmeof_client_cert, True).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}nvmeof_root_ca_cert', json.dumps({'nvmeof.foo': Cert(nvmeof_root_ca_cert, True).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}grafana_cert', json.dumps({'host-1': Cert(grafana_cert_host_1, True).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_CERT_PREFIX}grafana_cert', json.dumps({'host-1': Cert(grafana_cert_host_1, True).to_json(),
                                                                                'host-2': Cert(grafana_cert_host_2, True).to_json()}))
        ]
        _set_store.assert_has_calls(expected_calls)

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_tlsobject_store_cert_ls(self, _set_store, cephadm_module: CephadmOrchestrator):

        def get_generated_cephadm_cert_info_1():
            return {
                "extensions": {
                    "basicConstraints": {"ca": False, "path_length": None},
                    "subjectAltName": {
                        "DNSNames": ["ceph-node-0.ceph-orch", "grafana_servers"],
                        "IPAddresses": ["192.168.100.100"],
                    },
                },
                "issuer": {"commonName": "cephadm-root", "organizationName": "Ceph"},
                "public_key": {"key_size": 4096, "key_type": "RSA"},
                "subject": {"commonName": "192.168.100.100"},
                "validity": {
                    "not_after": "2028-01-21T16:06:56",
                    "not_before": "2025-01-21T16:06:56",
                    "remaining_days": 1092,
                },
            }

        def get_generated_cephadm_cert_info_2():
            return {
                "extensions": {
                    "basicConstraints": {"ca": False, "path_length": None},
                    "subjectAltName": {
                        "DNSNames": ["ceph-node-2.ceph-orch", "grafana_servers"],
                        "IPAddresses": ["192.168.100.102"],
                    },
                },
                "issuer": {"commonName": "cephadm-root", "organizationName": "Ceph"},
                "public_key": {"key_size": 4096, "key_type": "RSA"},
                "subject": {"commonName": "192.168.100.102"},
                "validity": {
                    "not_after": "2028-01-24T12:21:22",
                    "not_before": "2025-01-24T12:21:22",
                    "remaining_days": 1094,
                },
            }

        def compare_certls_dicts(expected_ls):
            actual_ls = cephadm_module.cert_mgr.cert_ls(include_datails=True)
            assert actual_ls.keys() == expected_ls.keys()
            for svc_cert_name, value in expected_ls.items():
                expected_certs_entry = value['certificates']
                actual_certs_entry = actual_ls[svc_cert_name]['certificates']
                scope = value['scope']
                if scope == 'global':
                    assert 'validity' in expected_certs_entry
                    validity = expected_certs_entry['validity']
                    assert re.match(validity['not_after'], actual_certs_entry['validity']['not_after'])
                    assert re.match(validity['not_before'], actual_certs_entry['validity']['not_before'])
                else:  # case of per service/host certificates
                    for target, cert_info in expected_certs_entry.items():
                        assert 'validity' in cert_info
                        validity = cert_info['validity']
                        assert re.match(validity['not_after'], actual_certs_entry[target]['validity']['not_after'])
                        assert re.match(validity['not_before'], actual_certs_entry[target]['validity']['not_before'])

        expected_ls = {
            "cephadm_root_ca_cert": {
                "certificates": {
                    "extensions": {
                        "basicConstraints": {"ca": True, "path_length": None},
                        "subjectAltName": {"DNSNames": [], "IPAddresses": ["::1"]},
                    },
                    "issuer": {
                        "commonName": "cephadm-root",
                        "organizationName": "Ceph",
                    },
                    "public_key": {"key_size": 4096, "key_type": "RSA"},
                    "subject": {
                        "commonName": "cephadm-root",
                        "organizationName": "Ceph",
                    },
                    "validity": {
                        "not_after": re.compile(".*"),
                        "not_before": re.compile(".*"),
                        "remaining_days": re.compile(".*"),
                    },
                },
                "scope": "global"
            }
        }

        # default certificate list (cephadm root CA)
        compare_certls_dicts(expected_ls)

        # Services with sevice_name target/scope
        cephadm_module.cert_mgr.save_cert('rgw_frontend_ssl_cert', CEPHADM_SELF_GENERATED_CERT_1, service_name='rgw.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('rgw_frontend_ssl_cert', CEPHADM_SELF_GENERATED_CERT_2, service_name='rgw.bar', user_made=True)
        expected_ls["rgw_frontend_ssl_cert"] = {
            "scope": "service",
            "certificates": {
                "rgw.foo": get_generated_cephadm_cert_info_1(),
                "rgw.bar": get_generated_cephadm_cert_info_2(),
            },
        }
        compare_certls_dicts(expected_ls)

        # Services with host target/scope
        cephadm_module.cert_mgr.save_cert('grafana_cert', CEPHADM_SELF_GENERATED_CERT_1, host='host1', user_made=True)
        cephadm_module.cert_mgr.save_cert('grafana_cert', CEPHADM_SELF_GENERATED_CERT_2, host='host2', user_made=True)
        expected_ls['grafana_cert'] = {
            'scope': 'host',
            'certificates': {
                'host1': get_generated_cephadm_cert_info_1(),
                'host2': get_generated_cephadm_cert_info_2(),
            },
        }
        compare_certls_dicts(expected_ls)

        # Services with global target/scope
        cephadm_module.cert_mgr.save_cert('mgmt_gw_cert', CEPHADM_SELF_GENERATED_CERT_1, user_made=True)
        cephadm_module.cert_mgr.save_cert('oauth2_proxy_cert', CEPHADM_SELF_GENERATED_CERT_2, user_made=True)
        expected_ls['mgmt_gw_cert'] = {'scope': 'global', 'certificates': get_generated_cephadm_cert_info_1()}
        expected_ls['oauth2_proxy_cert'] = {'scope': 'global', 'certificates': get_generated_cephadm_cert_info_2()}
        compare_certls_dicts(expected_ls)

        # nvmeof certificates
        cephadm_module.cert_mgr.save_cert('nvmeof_client_cert', CEPHADM_SELF_GENERATED_CERT_1, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_server_cert', CEPHADM_SELF_GENERATED_CERT_1, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_root_ca_cert', CEPHADM_SELF_GENERATED_CERT_2, service_name='nvmeof.foo', user_made=True)
        expected_ls.update(
            {
                "nvmeof_client_cert": {
                    "scope": "service",
                    "certificates": {
                        "nvmeof.foo": get_generated_cephadm_cert_info_1(),
                    },
                },
                "nvmeof_server_cert": {
                    "scope": "service",
                    "certificates": {
                        "nvmeof.foo": get_generated_cephadm_cert_info_1(),
                    },
                },
                "nvmeof_root_ca_cert": {
                    "scope": "service",
                    "certificates": {
                        "nvmeof.foo": get_generated_cephadm_cert_info_2(),
                    },
                },
            }
        )
        compare_certls_dicts(expected_ls)

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_tlsobject_store_save_key(self, _set_store, cephadm_module: CephadmOrchestrator):

        grafana_host1_key = 'fake-grafana-host1-key'
        grafana_host2_key = 'fake-grafana-host2-key'
        nvmeof_client_key = 'nvmeof-client-key'
        nvmeof_server_key = 'nvmeof-server-key'
        nvmeof_encryption_key = 'nvmeof-encryption-key'
        cephadm_module.cert_mgr.save_key('grafana_key', grafana_host1_key, host='host1')
        cephadm_module.cert_mgr.save_key('grafana_key', grafana_host2_key, host='host2')
        cephadm_module.cert_mgr.save_key('nvmeof_client_key', nvmeof_client_key, service_name='nvmeof.foo')
        cephadm_module.cert_mgr.save_key('nvmeof_server_key', nvmeof_server_key, service_name='nvmeof.foo')
        cephadm_module.cert_mgr.save_key('nvmeof_encryption_key', nvmeof_encryption_key, service_name='nvmeof.foo')

        expected_calls = [
            mock.call(f'{TLSOBJECT_STORE_KEY_PREFIX}grafana_key', json.dumps({'host1': PrivKey(grafana_host1_key).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_KEY_PREFIX}grafana_key', json.dumps({'host1': PrivKey(grafana_host1_key).to_json(),
                                                                              'host2': PrivKey(grafana_host2_key).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_KEY_PREFIX}nvmeof_client_key', json.dumps({'nvmeof.foo': PrivKey(nvmeof_client_key).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_KEY_PREFIX}nvmeof_server_key', json.dumps({'nvmeof.foo': PrivKey(nvmeof_server_key).to_json()})),
            mock.call(f'{TLSOBJECT_STORE_KEY_PREFIX}nvmeof_encryption_key', json.dumps({'nvmeof.foo': PrivKey(nvmeof_encryption_key).to_json()})),
        ]
        _set_store.assert_has_calls(expected_calls)

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_tlsobject_store_key_ls(self, _set_store, cephadm_module: CephadmOrchestrator):
        expected_ls = {
            'nvmeof_server_key': {
                'scope': 'service',
                'keys': {
                    'nvmeof.foo': {
                        'key_type': 'RSA',
                        'key_size': 4096
                    }
                }
            },
            'nvmeof_client_key': {
                'scope': 'service',
                'keys': {
                    'nvmeof.foo': {
                        'key_type': 'RSA',
                        'key_size': 4096
                    }
                }
            },
            'nvmeof_encryption_key': {
                'scope': 'service',
                'keys': {
                    'nvmeof.foo': {
                        'key_type': 'RSA',
                        'key_size': 2048
                    }
                }
            }
        }

        cephadm_module.cert_mgr.save_key('nvmeof_client_key', CEPHADM_SELF_GENERATED_KEY_4096, service_name='nvmeof.foo')
        cephadm_module.cert_mgr.save_key('nvmeof_server_key', CEPHADM_SELF_GENERATED_KEY_4096, service_name='nvmeof.foo')
        cephadm_module.cert_mgr.save_key('nvmeof_encryption_key', CEPHADM_SELF_GENERATED_KEY_2048, service_name='nvmeof.foo')
        assert cephadm_module.cert_mgr.key_ls() == expected_ls

        cephadm_module.cert_mgr.save_key('ingress_ssl_key', 'invalid_key', service_name='ingress.foo')
        assert 'Error parsing key' in cephadm_module.cert_mgr.key_ls()['ingress_ssl_key']['keys']['ingress.foo']['Error']

    @mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix")
    def test_tlsobject_store_load(self, _get_store_prefix, cephadm_module: CephadmOrchestrator):

        # Define certs and keys with their corresponding scopes
        certs = {
            'rgw_frontend_ssl_cert': ('rgw.foo', 'fake-rgw-cert', TLSObjectScope.SERVICE),
            'nvmeof_server_cert': ('nvmeof.foo', 'nvmeof-server-cert', TLSObjectScope.SERVICE),
            'nvmeof_client_cert': ('nvmeof.foo', 'nvmeof-client-cert', TLSObjectScope.SERVICE),
            'nvmeof_root_ca_cert': ('nvmeof.foo', 'nvmeof-root-ca-cert', TLSObjectScope.SERVICE),
            'ingress_ssl_cert': ('ingress', 'ingress-ssl-cert', TLSObjectScope.SERVICE),
            'iscsi_ssl_cert': ('iscsi', 'iscsi-ssl-cert', TLSObjectScope.SERVICE),
            'grafana_cert': ('host1', 'grafana-cert', TLSObjectScope.HOST),
            'mgmt_gw_cert': ('mgmt-gateway', 'mgmt-gw-cert', TLSObjectScope.GLOBAL),
            'oauth2_proxy_cert': ('oauth2-proxy', 'oauth2-proxy-cert', TLSObjectScope.GLOBAL),
        }
        unknown_certs = {
            'unknown_per_service_cert': ('unknown-svc.foo', 'unknown-cert', TLSObjectScope.SERVICE),
            'unknown_per_host_cert': ('unknown-host.foo', 'unknown-cert', TLSObjectScope.HOST),
            'unknown_global_cert': ('unknown-global.foo', 'unknown-cert', TLSObjectScope.GLOBAL),
            'cert_with_unknown_scope': ('unknown-global.foo', 'unknown-cert', TLSObjectScope.UNKNOWN),
        }

        keys = {
            'grafana_key': ('host1', 'fake-grafana-host1-key', TLSObjectScope.HOST),
            'nvmeof_server_key': ('nvmeof.foo', 'nvmeof-server-key', TLSObjectScope.SERVICE),
            'nvmeof_client_key': ('nvmeof.foo', 'nvmeof-client-key', TLSObjectScope.SERVICE),
            'nvmeof_encryption_key': ('nvmeof.foo', 'nvmeof-encryption-key', TLSObjectScope.SERVICE),
            'mgmt_gw_key': ('mgmt-gateway', 'mgmt-gw-key', TLSObjectScope.GLOBAL),
            'oauth2_proxy_key': ('oauth2-proxy', 'oauth2-proxy-key', TLSObjectScope.GLOBAL),
            'ingress_ssl_key': ('ingress', 'ingress-ssl-key', TLSObjectScope.SERVICE),
            'iscsi_ssl_key': ('iscsi', 'iscsi-ssl-key', TLSObjectScope.SERVICE),
        }
        unknown_keys = {
            'unknown_per_service_key': ('unknown-svc.foo', 'unknown-key', TLSObjectScope.SERVICE),
            'unknown_per_host_key': ('unknown-host.foo', 'unknown-key', TLSObjectScope.HOST),
            'unknown_global_key': ('unknown-global.foo', 'unknown-key', TLSObjectScope.GLOBAL),
            'key_with_unknown_scope': ('unknown-global.foo', 'unknown-key', TLSObjectScope.UNKNOWN),
        }

        # Mock function to simulate store behavior
        def _fake_prefix_store(key):
            from itertools import chain
            if key == 'cert_store.cert.':
                return {
                    f'{TLSOBJECT_STORE_CERT_PREFIX}{cert_name}': json.dumps(
                        {target: Cert(cert_value, True).to_json()} if scope != TLSObjectScope.GLOBAL
                        else Cert(cert_value, True).to_json()
                    )
                    for cert_name, (target, cert_value, scope) in chain(certs.items(), unknown_certs.items())
                }
            elif key == 'cert_store.key.':
                return {
                    f'{TLSOBJECT_STORE_KEY_PREFIX}{key_name}': json.dumps(
                        {target: PrivKey(key_value).to_json()} if scope != TLSObjectScope.GLOBAL
                        else PrivKey(key_value).to_json()
                    )
                    for key_name, (target, key_value, scope) in chain(keys.items(), unknown_keys.items())
                }
            else:
                raise Exception(f'Unexpected key access in store: {key}')

        # Inject the mock store behavior and the cert manager
        _get_store_prefix.side_effect = _fake_prefix_store
        cephadm_module._init_cert_mgr()

        # Validate certificates in cert_store
        for cert_name, (target, cert_value, scope) in certs.items():
            assert cert_name in cephadm_module.cert_mgr.cert_store.known_entities
            if scope == TLSObjectScope.GLOBAL:
                assert cephadm_module.cert_mgr.cert_store.known_entities[cert_name] == Cert(cert_value, True)
            else:
                assert cephadm_module.cert_mgr.cert_store.known_entities[cert_name][target] == Cert(cert_value, True)

        # Validate keys in key_store
        for key_name, (target, key_value, scope) in keys.items():
            assert key_name in cephadm_module.cert_mgr.key_store.known_entities
            if scope == TLSObjectScope.GLOBAL:
                assert cephadm_module.cert_mgr.key_store.known_entities[key_name] == PrivKey(key_value)
            else:
                assert cephadm_module.cert_mgr.key_store.known_entities[key_name][target] == PrivKey(key_value)

        # Check unknown certificates are not loaded
        for unknown_cert in unknown_certs:
            assert unknown_cert not in cephadm_module.cert_mgr.cert_store.known_entities

        # Check unknown keys are not loaded
        for unknown_key in unknown_keys:
            assert unknown_key not in cephadm_module.cert_mgr.key_store.known_entities

    def test_tlsobject_store_get_cert_key(self, cephadm_module: CephadmOrchestrator):

        rgw_frontend_rgw_foo_host2_cert = 'fake-rgw-cert'
        nvmeof_client_cert = 'fake-nvmeof-client-cert'
        nvmeof_server_cert = 'fake-nvmeof-server-cert'
        cephadm_module.cert_mgr.save_cert('rgw_frontend_ssl_cert', rgw_frontend_rgw_foo_host2_cert, service_name='rgw.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_server_cert', nvmeof_server_cert, service_name='nvmeof.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_client_cert', nvmeof_client_cert, service_name='nvmeof.foo', user_made=True)

        assert cephadm_module.cert_mgr.get_cert('rgw_frontend_ssl_cert', service_name='rgw.foo') == rgw_frontend_rgw_foo_host2_cert
        assert cephadm_module.cert_mgr.get_cert('nvmeof_server_cert', service_name='nvmeof.foo') == nvmeof_server_cert
        assert cephadm_module.cert_mgr.get_cert('nvmeof_client_cert', service_name='nvmeof.foo') == nvmeof_client_cert
        assert cephadm_module.cert_mgr.get_cert('grafana_cert', host='host1') is None
        assert cephadm_module.cert_mgr.get_cert('iscsi_ssl_cert', service_name='iscsi.foo') is None
        assert cephadm_module.cert_mgr.get_cert('nvmeof_root_ca_cert', service_name='nvmeof.foo') is None

        with pytest.raises(TLSObjectException, match='Attempted to access cert for unknown entity'):
            cephadm_module.cert_mgr.get_cert('unknown_entity')
        with pytest.raises(TLSObjectException, match='Need host to access cert for entity'):
            cephadm_module.cert_mgr.get_cert('grafana_cert')
        with pytest.raises(TLSObjectException, match='Need service name to access cert for entity'):
            cephadm_module.cert_mgr.get_cert('rgw_frontend_ssl_cert', host='foo')

        grafana_host1_key = 'fake-grafana-host1-cert'
        nvmeof_server_key = 'nvmeof-server-key'
        nvmeof_encryption_key = 'nvmeof-encryption-key'
        cephadm_module.cert_mgr.save_key('grafana_key', grafana_host1_key, host='host1')
        cephadm_module.cert_mgr.save_key('grafana_key', grafana_host1_key, host='host1')
        cephadm_module.cert_mgr.save_key('nvmeof_server_key', nvmeof_server_key, service_name='nvmeof.foo')
        cephadm_module.cert_mgr.save_key('nvmeof_encryption_key', nvmeof_encryption_key, service_name='nvmeof.foo')

        assert cephadm_module.cert_mgr.get_key('grafana_key', host='host1') == grafana_host1_key
        assert cephadm_module.cert_mgr.get_key('nvmeof_server_key', service_name='nvmeof.foo') == nvmeof_server_key
        assert cephadm_module.cert_mgr.get_key('nvmeof_client_key', service_name='nvmeof.foo') is None
        assert cephadm_module.cert_mgr.get_key('nvmeof_encryption_key', service_name='nvmeof.foo') == nvmeof_encryption_key

        with pytest.raises(TLSObjectException, match='Attempted to access privkey for unknown entity'):
            cephadm_module.cert_mgr.get_key('unknown_entity')
        with pytest.raises(TLSObjectException, match='Need host to access privkey for entity'):
            cephadm_module.cert_mgr.get_key('grafana_key')

    def test_tlsobject_store_rm_cert(self, cephadm_module: CephadmOrchestrator):

        # Save some certificates and ensure certificates are present
        cephadm_module.cert_mgr.save_cert('rgw_frontend_ssl_cert', 'fake-rgw-cert', service_name='rgw.foo', user_made=True)
        cephadm_module.cert_mgr.save_cert('nvmeof_server_cert', 'fake-nvmeof-server-cert', service_name='nvmeof.foo', user_made=True)
        assert cephadm_module.cert_mgr.get_cert('rgw_frontend_ssl_cert', service_name='rgw.foo') == 'fake-rgw-cert'
        assert cephadm_module.cert_mgr.get_cert('nvmeof_server_cert', service_name='nvmeof.foo') == 'fake-nvmeof-server-cert'

        # Remove certificates and ensure certificates are removed
        cephadm_module.cert_mgr.rm_cert('rgw_frontend_ssl_cert', service_name='rgw.foo')
        cephadm_module.cert_mgr.rm_cert('nvmeof_server_cert', service_name='nvmeof.foo')
        assert cephadm_module.cert_mgr.get_cert('rgw_frontend_ssl_cert', service_name='rgw.foo') is None
        assert cephadm_module.cert_mgr.get_cert('nvmeof_server_cert', service_name='nvmeof.foo') is None

    def test_tlsobject_store_rm_key(self, cephadm_module: CephadmOrchestrator):

        # Save some keys and ensure keys are present
        cephadm_module.cert_mgr.save_key('grafana_key', 'fake-grafana-host1-key', host='host1')
        cephadm_module.cert_mgr.save_key('nvmeof_server_key', 'fake-nvmeof-server-key', service_name='nvmeof.foo')
        assert cephadm_module.cert_mgr.get_key('grafana_key', host='host1') == 'fake-grafana-host1-key'
        assert cephadm_module.cert_mgr.get_key('nvmeof_server_key', service_name='nvmeof.foo') == 'fake-nvmeof-server-key'

        # Remove keys and ensure keys are removed
        cephadm_module.cert_mgr.rm_key('grafana_key', host='host1')
        cephadm_module.cert_mgr.rm_key('nvmeof_server_key', service_name='nvmeof.foo')
        assert cephadm_module.cert_mgr.get_key('grafana_key', host='host1') is None
        assert cephadm_module.cert_mgr.get_key('nvmeof_server_key', service_name='nvmeof.foo') is None

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_expired_certificate_detection(self, _set_store, cephadm_module: CephadmOrchestrator):
        """ Test that expired certificates are flagged correctly """
        cert_mgr = cephadm_module.cert_mgr
        cert_info = cert_mgr.check_certificate_state("test_service", "test_target", EXPIRED_CERT, MATCHING_KEY)
        assert not cert_info.is_valid
        assert "expired" in cert_info.error_info.lower()

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_invalid_certificate_detection(self, _set_store, cephadm_module: CephadmOrchestrator):
        """ Test that invalid certificates are correctly detected """
        cert_mgr = cephadm_module.cert_mgr
        cert_info = cert_mgr.check_certificate_state("test_service", "test_target", INVALID_CERT, MATCHING_KEY)
        assert not cert_info.is_valid

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_non_matching_key(self, _set_store, cephadm_module: CephadmOrchestrator):
        """ Test that certificates with non-matching keys are flagged """
        cert_mgr = cephadm_module.cert_mgr
        cert_info = cert_mgr.check_certificate_state("test_service", "test_target", CEPHADM_SELF_GENERATED_CERT_1, NON_MATCHING_KEY)
        assert not cert_info.is_valid
        assert 'invalid cert/key pair' in cert_info.error_info.lower()

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_certificate_renewal_for_self_signed(self, _set_store, cephadm_module: CephadmOrchestrator):
        """ Test that cephadm-signed certificates close to expiration are renewed """
        cert_mgr = cephadm_module.cert_mgr

        # for services with host scope
        cert_mgr.save_cert('grafana_cert', EXPIRED_CERT, host="test_host", user_made=True)
        cert_info = CertInfo("grafana_cert", "test_host", is_valid=True, is_close_to_expiration=True, days_to_expiration=5)
        cert_obj = Cert(EXPIRED_CERT, user_made=False)
        with mock.patch.object(cert_mgr.ssl_certs, "renew_cert", return_value=("mock_new_cert", "mock_new_key")) as renew_mock:
            cert_mgr._renew_self_signed_certificate(cert_info, cert_obj)
            renew_mock.assert_called_once()

        # for services with service scope
        cert_mgr.save_cert('ingress_ssl_cert', EXPIRED_CERT, service_name="test_service", user_made=True)
        cert_info = CertInfo('ingress_ssl_cert', "test_service", is_valid=True, is_close_to_expiration=True, days_to_expiration=5)
        cert_obj = Cert(EXPIRED_CERT, user_made=False)
        with mock.patch.object(cert_mgr.ssl_certs, "renew_cert", return_value=("mock_new_cert", "mock_new_key")) as renew_mock:
            cert_mgr._renew_self_signed_certificate(cert_info, cert_obj)
            renew_mock.assert_called_once()

        # for services with global scope
        cert_mgr.save_cert('mgmt_gw_cert', EXPIRED_CERT, user_made=True)
        cert_info = CertInfo('mgmt_gw_cert', "test_service", is_valid=True, is_close_to_expiration=True, days_to_expiration=5)
        cert_obj = Cert(EXPIRED_CERT, user_made=False)
        with mock.patch.object(cert_mgr.ssl_certs, "renew_cert", return_value=("mock_new_cert", "mock_new_key")) as renew_mock:
            cert_mgr._renew_self_signed_certificate(cert_info, cert_obj)
            renew_mock.assert_called_once()

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_health_errors_appending(self, _set_store, cephadm_module: CephadmOrchestrator):
        """ Test in case of appending new errors we also report previous ones """
        cert_mgr = cephadm_module.cert_mgr

        # Test health error is raised if any invalid cert is detected
        problematic_certs = [
            CertInfo("test_service_1", "target_1", user_made=True, is_valid=False, error_info="expired"),
            CertInfo("test_service_2", "target_2", is_valid=False, error_info="invalid format"),
        ]
        with mock.patch.object(cert_mgr.mgr, "set_health_error") as health_mock:
            cert_mgr._notify_certificates_health_status(problematic_certs)
            health_mock.assert_called_with('CEPHADM_CERT_ERROR',
                                           'Detected 2 cephadm certificate(s) issues: 1 invalid, 1 expired',
                                           2,
                                           ["Certificate 'test_service_1 (target_1)' (user-made) has expired",
                                            "Certificate 'test_service_2 (target_2)' (cephadm-signed) is not valid (error: invalid format)"])

        # Test in case of appending new errors we also report previous ones
        problematic_certs = [
            CertInfo("test_service_3", "target_3", is_valid=True, is_close_to_expiration=True),
        ]
        with mock.patch.object(cert_mgr.mgr, "set_health_error") as health_mock:
            cert_mgr._notify_certificates_health_status(problematic_certs)
            health_mock.assert_called_with('CEPHADM_CERT_ERROR',
                                           'Detected 3 cephadm certificate(s) issues: 1 invalid, 1 expired, 1 expiring',
                                           3,
                                           ["Certificate 'test_service_1 (target_1)' (user-made) has expired",
                                            "Certificate 'test_service_2 (target_2)' (cephadm-signed) is not valid (error: invalid format)",
                                            "Certificate 'test_service_3 (target_3)' (cephadm-signed) is about to expire (remaining days: 0)"])

    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_health_warning_on_bad_certificates(self, _set_store, cephadm_module: CephadmOrchestrator):
        """ Test that invalid and expired certificates trigger health warnings """
        cert_mgr = cephadm_module.cert_mgr

        # Test health error is raised if any invalid cert is detected
        problematic_certs = [
            CertInfo("test_service", "test_target", is_valid=False, error_info="expired"),
            CertInfo("test_service", "test_target", is_valid=False, error_info="invalid format"),
        ]

        with mock.patch.object(cert_mgr.mgr, "set_health_error") as health_mock:
            cert_mgr._notify_certificates_health_status(problematic_certs)
            health_mock.assert_called_once()

        # Test health warning is raised if valid but close to expire cert is detected
        problematic_certs = [
            CertInfo("test_service", "test_target", is_valid=True, is_close_to_expiration=True, error_info="about to expire"),
        ]
        cert_mgr.certificates_health_report = []
        with mock.patch.object(cert_mgr.mgr, "set_health_warning") as health_mock:
            cert_mgr._notify_certificates_health_status(problematic_certs)
            health_mock.assert_called_once()

        # Test in case of no bad certificates issues the error is cleared correctly
        problematic_certs = []
        cert_mgr.certificates_health_report = []
        with mock.patch.object(cert_mgr.mgr, "set_health_warning") as warning_mock, \
             mock.patch.object(cert_mgr.mgr, "set_health_error") as error_mock, \
             mock.patch.object(cert_mgr.mgr, "remove_health_warning") as remove_warning_mock:

            cert_mgr._notify_certificates_health_status(problematic_certs)
            # Ensure that neither warnings nor errors were raised
            warning_mock.assert_not_called()
            error_mock.assert_not_called()
            remove_warning_mock.assert_called_once_with(CertMgr.CEPHADM_CERTMGR_HEALTH_ERR)


class MockTLSObject(TLSObjectProtocol):
    STORAGE_PREFIX = "mocktls"

    def __init__(self, data: str = "", user_made: bool = False):
        self.data = data
        self.user_made = user_made

    def __bool__(self):
        return bool(self.data)

    @staticmethod
    def to_json(obj):
        return {"data": obj.data, "user_made": obj.user_made}

    @staticmethod
    def from_json(json_data):
        return MockTLSObject(json_data["data"], json_data["user_made"])


class MockCephadmOrchestrator:
    def __init__(self):
        self.store = {}

    def set_store(self, key, value):
        self.store[key] = value

    def get_store_prefix(self, prefix):
        return {k: v for k, v in self.store.items() if k.startswith(prefix)}


class TestTLSObjectStore(unittest.TestCase):
    def setUp(self):
        known_entities = {
            TLSObjectScope.GLOBAL: ["global_cert_1", "global_cert_2"],
            TLSObjectScope.SERVICE: ["per_service1", "per_service2"],
            TLSObjectScope.HOST: ["per_host1", "per_host2"],
        }
        self.mgr = MockCephadmOrchestrator()
        self.store = TLSObjectStore(self.mgr, MockTLSObject, known_entities)

    def test_save_and_get_tlsobject(self):
        self.store.save_tlsobject("per_service1", "my_cert_data", service_name="my_service")
        obj = self.store.get_tlsobject("per_service1", service_name="my_service")
        self.assertIsNotNone(obj)
        self.assertEqual(obj.data, "my_cert_data")

    def test_remove_tlsobject(self):
        self.store.save_tlsobject("per_host1", "cert_data", host="my_host")
        self.store.rm_tlsobject("per_host1", host="my_host")
        obj = self.store.get_tlsobject("per_host1", host="my_host")
        self.assertIsNone(obj)

    def test_get_tlsobject_scope_and_target(self):
        scope, target = self.store.get_tlsobject_scope_and_target("per_service1", service_name="my_service")
        self.assertEqual(scope, TLSObjectScope.SERVICE)
        self.assertEqual(target, "my_service")

        scope, target = self.store.get_tlsobject_scope_and_target("per_host1", host="my_host")
        self.assertEqual(scope, TLSObjectScope.HOST)
        self.assertEqual(target, "my_host")

        scope, target = self.store.get_tlsobject_scope_and_target("global_cert_1")
        self.assertEqual(scope, TLSObjectScope.GLOBAL)
        self.assertEqual(target, None)

    def test_list_tlsobjects(self):
        self.store.save_tlsobject("global_cert_1", "cert_data1")
        self.store.save_tlsobject("global_cert_2", "cert_data2")
        self.store.save_tlsobject("per_service1", "cert_data1", service_name="my_service_1")
        self.store.save_tlsobject("per_host1", "cert_data2", host="my_host")
        tlsobjects = self.store.list_tlsobjects()
        expected_entries = {("global_cert_1", None),
                            ("global_cert_2", None),
                            ("per_service1", "my_service_1"),
                            ("per_host1", "my_host")}
        actual_entries = {(entity, target) for entity, tlsobject, target in tlsobjects if isinstance(tlsobject, MockTLSObject)}
        self.assertEqual(len(tlsobjects), 4)
        assert expected_entries == actual_entries

    def test_invalid_entity_access(self):
        with self.assertRaises(TLSObjectException):
            self.store.get_tlsobject("unknown_entity")

    def test_validate_tlsobject_entity(self):
        with self.assertRaises(TLSObjectException):
            self.store._validate_tlsobject_entity("unknown_entity")
        with self.assertRaises(TLSObjectException):
            self.store._validate_tlsobject_entity("per_host1")
        with self.assertRaises(TLSObjectException):
            self.store._validate_tlsobject_entity("per_service1")
