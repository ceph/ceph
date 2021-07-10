import logging
import os
import re

from jinja2 import Template
from kcli_handler import check_cephadm_status, execute_kcli_cmd

KCLI_PLANS_DIR = "generated_plans"
KCLI_PLAN_NAME = "behave_test_plan"

Configuration = {
    "nodes": 1,
    "pool": "default",
    "network": "default",
    "domain": "cephlab.com",
    "prefix": "ceph",
    "numcpus": 1,
    "memory": 1024,
    "image": "fedora33",
    "notify": False,
    "admin_password": "password",
    "disks": [150, 3],
}


def _write_file(file_path, data):
    with open(file_path, "w") as file:
        file.write(data)


def _read_file(file_path):
    file = open(file_path, "r")
    data = "".join(file.readlines())
    file.close()
    return data


def _loaded_templates():
    temp_dir = os.path.join(os.getcwd(), "template")
    logging.info("Loading templates")
    kcli = _read_file(os.path.join(temp_dir, "kcli_plan_template"))
    script = _read_file(os.path.join(temp_dir, "bootstrap_script_template"))
    kcli_temp = Template(kcli)
    return (kcli_temp, script)


def clean_generated(dir_path):
    logging.info("Deleting generated files")
    for file in os.listdir(dir_path):
        os.remove(os.path.join(dir_path, file))
    os.rmdir(dir_path)


def _parse_value(value):
    if value.isnumeric():
        return int(value)

    if value.endswith("gb"):
        return int(value.replace("gb", "")) * 1024
    elif value.endswith("mb"):
        return value.replace("mb", "")
    return value


def _parse_feature_specifications(features):
    parsed_str = re.search(
        r"(?P<nodes>[\d]+) nodes with (?P<memory>[\w\.-]+) ram",
        features.lower(),
    )
    if parsed_str:
        for spec_key in parsed_str.groupdict().keys():
            Configuration[spec_key] = _parse_value(parsed_str.group(spec_key))
    parsed_str = re.search(r"(?P<numcpus>[\d]+) cpus", features.lower())
    if parsed_str:
        Configuration["numcpus"] = parsed_str.group("numcpus")
    parsed_str = re.search(
        r"(?P<disk>[\d]+) storage devices of (?P<volume>[\w\.-]+)Gb each",
        features,
    )
    if parsed_str:
        Configuration["disks"] = [
            _parse_value(parsed_str.group("volume"))
        ] * _parse_value(parsed_str.group("disk"))
    parsed_str = re.search(r"(?P<image>[\w\.-]+) image", features.lower())
    if parsed_str:
        Configuration["image"] = parsed_str.group("image")
    return Configuration


def _handle_kcli_plan(command_type, plan_file_path=None):
    _output = None
    print("Creating kcli plans...")
    if command_type == "create":
        _output = execute_kcli_cmd(
            f"create plan -f {plan_file_path} {KCLI_PLAN_NAME}"
        )
    elif command_type == "delete":
        _output = execute_kcli_cmd(f"delete plan {KCLI_PLAN_NAME} -y")
    logging.info(f"kcli output : {_output}")


def before_feature(context, feature):
    kcli_plans_dir_path = os.path.join(os.getcwd(), KCLI_PLANS_DIR)
    if not os.path.exists(kcli_plans_dir_path):
        os.mkdir(kcli_plans_dir_path)

    features = " ".join(
        [line for line in feature.description if line.startswith("-")]
    )
    feature_config = _parse_feature_specifications("".join(features))
    kcli_plan_path = os.path.join(kcli_plans_dir_path, "gen_kcli_plan.yml")
    script_path = os.path.join(kcli_plans_dir_path, "bootstrap_cluster_dev.sh")
    loaded_kcli, loaded_script = _loaded_templates()
    gen_kcli = loaded_kcli.render(feature_config)

    _write_file(script_path, loaded_script)
    _write_file(kcli_plan_path, gen_kcli)
    _handle_kcli_plan("create", os.path.relpath(kcli_plan_path))
    check_cephadm_status()


def after_feature(context, feature):
    if os.path.exists(KCLI_PLANS_DIR):
        clean_generated(os.path.abspath(KCLI_PLANS_DIR))
    _handle_kcli_plan("delete")
