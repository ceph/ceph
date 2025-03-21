import json, sys
from jinja2 import Environment, FileSystemLoader

if (len(sys.argv[1:]) < 2):
    raise Exception(
        "Need to pass two parameters, like: python3 grafana-libsonnet-to-json.py <input_json_path> <output_libsonnet_path>"
    )

json_path = sys.argv[1] # example: dashboards_out/new.json
output_path = sys.argv[2] # example: dashboards/new.libsonnet

with open(json_path, "r") as f:
    data = json.load(f)

env = Environment(loader=FileSystemLoader("."))
template = env.get_template("grafana-template.libsonnet.j2")

output = template.render(data)

with open(output_path, "w") as f:
    f.write(output)

print(f"DONE! Successfully converted libsonnet to json!")
