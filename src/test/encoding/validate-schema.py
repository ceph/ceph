#!/usr/bin/env python3

import sys
import json
import jsonschema

schema_dir = sys.argv[1]

with open(sys.argv[2]) as fp:
    document = json.load(fp)

with open(sys.argv[3]) as fp:
    schema = json.load(fp)

resolver = jsonschema.RefResolver("file://{}/".format(schema_dir), None)
jsonschema.validate(document, schema, resolver = resolver)
