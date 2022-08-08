#!/bin/bash

MGR_MODULE_PATH=src/pybind/mgr
MGR_REQUIREMENTS=$MGR_MODULE_PATH/requirements-mgr.txt

export PYPACKAGES_FOLDER=$MGR_MODULE_PATH/__pypackages__

function install-pip-packages() { 
  mkdir -p $PYPACKAGES_FOLDER
  pip install -r $MGR_REQUIREMENTS --target $PYPACKAGES_FOLDER/lib/python3/site-packages 

}
