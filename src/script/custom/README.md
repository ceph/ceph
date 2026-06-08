# Custom image scripts

Use this directory for scripts passed to `src/script/build-with-container.py`
with `--custom-image-script`.

Example:

```bash
src/script/build-with-container.py \
  -d centos9 \
  --custom-image-script src/script/custom/my-extra-setup.sh \
  -e build
```

The script runs near the end of `src/script/buildcontainer-setup.sh` while
building the container image.
