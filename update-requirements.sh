#!/bin/bash

pip-compile $@ requirements.in
sed -i'' -e '/^-e / d' -e 's/-r requirements.in/teuthology/g' requirements.txt