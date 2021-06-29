#!/usr/bin/env bash

cp cephadm.py __main__.py
zip ca.zip __main__.py
echo '#!/usr/bin/env python3' | cat - ca.zip >cephadm
chmod +x cephadm
rm __main__.py ca.zip
