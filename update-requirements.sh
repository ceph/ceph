#!/bin/bash

pip-compile $@ requirements.in -qo- | sed -e '/^-e / d' -e 's/-r requirements.in/teuthology/g' > requirements.txt
