#!/usr/bin/env bash
set -ex

echo "getting iogen"
wget http://download.ceph.com/qa/iogen_3.1p0.tar
tar -xf iogen_3.1p0.tar
cd iogen_3.1p0

echo "patching Makefile to remove conflicting flags"
sed -i 's/-Dstrlcpy=strncpy//g' Makefile
sed -i 's/-Darc4random=rand//g' Makefile

echo "injecting standalone wrapper functions into src/iogen.c"
cat << 'EOF' > patch.py
with open("src/iogen.c", "r") as f:
    content = f.read()

# Replace all occurrences in the source file to avoid collision with standard headers
content = content.replace("arc4random(", "my_arc4random(")
content = content.replace("strlcpy(", "my_strlcpy(")

header = """#include <stdlib.h>
#include <string.h>
#include <stdint.h>

static inline uint32_t my_arc4random(void) {
    return (uint32_t)rand();
}

static inline size_t my_strlcpy(char *dst, const char *src, size_t size) {
    size_t srclen = strlen(src);
    if (size > 0) {
        size_t len = (srclen >= size) ? size - 1 : srclen;
        memcpy(dst, src, len);
        dst[len] = '\\0';
    }
    return srclen;
}
"""

with open("src/iogen.c", "w") as f:
    f.write(header + "\n" + content)
EOF

python3 patch.py
rm patch.py

echo "making iogen"
make

echo "running iogen"
./iogen -n 5 -s 2g
echo "sleep for 10 min"
sleep 600
echo "stopping iogen"
./iogen -k

echo "OK"
