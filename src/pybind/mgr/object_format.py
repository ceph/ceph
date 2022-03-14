# object_format.py provides types and functions for working with
# requested output formats such as JSON, YAML, etc.

import enum

class Format(enum.Enum):
    plain = 'plain'
    json = 'json'
    json_pretty = 'json-pretty'
    yaml = 'yaml'
    xml_pretty = 'xml-pretty'
    xml = 'xml'

