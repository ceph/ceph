#!/usr/bin/env bash
python -m pip install --editable="file://`pwd`/../python-common"
python -m pip install $@
