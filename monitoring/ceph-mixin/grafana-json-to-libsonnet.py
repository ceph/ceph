import argparse, textwrap
from pathlib import Path
import json
import sys
import logging
from jinja2 import Environment, FileSystemLoader

GRAFANA_TEMPLATE = "grafana-template.libsonnet.j2"

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s: %(message)s", stream=sys.stdout
)
logger = logging.getLogger(__name__)


parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter, 
    description=textwrap.dedent("""Tool to convert grafana JSON to libsonnet.
Example: python3 grafana-libsonnet-to-json.py dashboards_out/test.json --debug
Example: python3 grafana-libsonnet-to-json.py dashboards_out/test.json -o dashboards/output.libsonnet """),
)
parser.add_argument("json_path")
parser.add_argument("-o", "--output")
parser.add_argument("-v", "--verbose", action="store_true")


def read_json(path):
    with open(str(path), "r") as f:
        json_data = json.load(f)
    return json_data

def write_output(path, data):
    with open(str(path), "w") as f:
        f.write(data)

def convert(json_data, json_file_name):
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template(GRAFANA_TEMPLATE)
    libsonnet_data = template.render(json_data, logger=logger, json_file=json_file_name)
    return libsonnet_data

def main():
    cli = parser.parse_args()

    if cli.verbose: 
            logger.setLevel(logging.DEBUG)

    input_path = cli.json_path 
    input_path = Path(input_path)
    if not input_path.exists():
        raise Exception(f'Cannot find input JSON file at: {input_path}')

    output_path = cli.output
    if not output_path:
        output_path = input_path.with_suffix(".libsonnet")


    json_data = read_json(str(input_path))
    libsonnet_data = convert(json_data, input_path.name)
    write_output(str(output_path), libsonnet_data)

    print(f"Successfully converted! Find output libsonnet at: '{output_path}'.")

main() 

