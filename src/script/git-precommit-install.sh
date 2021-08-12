#!/usr/bin/env bash

# install git precommit hooks https://pre-commit.com/
curl https://pre-commit.com/install-local.py | python -
pre-commit install > /dev/null
