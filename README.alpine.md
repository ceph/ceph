# Alpine Build (Experimental)

## Dev Env Setup

```
apk --update add bash sudo git
git clone https://github.com/ceph/ceph
```

### Build

```
./run-make-check.sh -DWITH_EMBEDDED=OFF -DWITH_SYSTEM_BOOST=ON -DWITH_LTTNG=OFF -DWITH_REENTRANT_STRSIGNAL=ON -DWITH_THREAD_SAFE_RES_QUERY=ON
```

### Packaging

```
./make-apk.sh
```

### Docker

```
cd ceph/src

./test/docker-test.sh --os-type alpine --os-version edge ./make-apk.sh

or

./test/docker-test.sh --os-type alpine --os-version edge -- ./run-make-check.sh -DWITH_EMBEDDED=OFF -DWITH_SYSTEM_BOOST=ON -DWITH_LTTNG=OFF -DWITH_REENTRANT_STRSIGNAL=ON -DWITH_THREAD_SAFE_RES_QUERY=ON

```

## Known Issues

- Uses musl libc malloc because musl does not currently support replacing malloc implementation. see https://bugs.alpinelinux.org/issues/5389
- No backtrace support need to look at libunwind integration
