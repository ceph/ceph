#!/bin/bash

echo "-e .[orchestra,test]" | pip-compile $@ - -qo- | sed '/^-e / d' > requirements.txt
