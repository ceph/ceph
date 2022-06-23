import logging
import os
import re

from jinja2 import Template
from kcli_handler import is_bootstrap_script_complete, execute_kcli_cmd

KCLI_PLANS_DIR = "generated_plans"
KCLI_PLAN_NAME = "behave_test_plan"

Kcli_Config = {
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

Bootstrap_Config = {
    "configure_osd": False
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
    return (
        Template(kcli),
        Template(script)
    )


def _clean_generated(dir_path):
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


def _parse_to_config_dict(values, config):
    for key in values.keys():
        config[key] = _parse_value(values[key])


def _parse_vm_description(specs):
    """
    Parse's vm specfication description into configuration dictionary
    """
    kcli_config = Kcli_Config.copy()
    parsed_str = re.search(
        r"(?P<nodes>[\d]+) nodes with (?P<memory>[\w\.-]+) ram",
        specs.lower(),
    )
    if parsed_str:
        for spec_key in parsed_str.groupdict().keys():
            kcli_config[spec_key] = _parse_value(parsed_str.group(spec_key))
    parsed_str = re.search(r"(?P<numcpus>[\d]+) cpus", specs.lower())
    if parsed_str:
        kcli_config["numcpus"] = parsed_str.group("numcpus")
    parsed_str = re.search(
        r"(?P<disk>[\d]+) storage devices of (?P<volume>[\w\.-]+)Gb each",
        specs,
    )
    if parsed_str:
        kcli_config["disks"] = [
            _parse_value(parsed_str.group("volume"))
        ] * _parse_value(parsed_str.group("disk"))
    parsed_str = re.search(r"(?P<image>[\w\.-]+) image", specs.lower())
    if parsed_str:
        kcli_config["image"] = parsed_str.group("image")
    return kcli_config


def _parse_ceph_description(specs):
    """
    Parse the ceph bootstrap script configuration descriptions.
    """
    bootstrap_script_config = Bootstrap_Config.copy()
    parsed_str = re.search(
        r"OSD (?P<osd>[\w\.-]+)", specs
    )
    if parsed_str:
        bootstrap_script_config["configure_osd"] = True if _parse_value(
            parsed_str.group("osd")
        ) else False
    return bootstrap_script_config


def _handle_kcli_plan(command_type, plan_file_path=None):
    """
    Executes the kcli vm create and delete command according
    to the provided configuration.
    """
    op = None
    if command_type == "create":
        # TODO : Before creating kcli plan check for existing kcli plans
        op, code = execute_kcli_cmd(
            f"create plan -f {plan_file_path} {KCLI_PLAN_NAME}"
        )
        if code:
            print(f"Failed to create kcli plan\n Message: {op}")
            exit(1)
    elif command_type == "delete":
        op, code = execute_kcli_cmd(f"delete plan {KCLI_PLAN_NAME} -y")
    print(op)


def has_ceph_configuration(descriptions, config_line):
    """
    Checks for ceph cluster configuration in descriptions.
    """
    index_config = -1
    for line in descriptions:
        if line.lower().startswith(config_line):
            index_config = descriptions.index(line)

    if index_config != -1:
        return (
            descriptions[:index_config],
            descriptions[index_config:],
        )
    return (
        descriptions,
        None,
    )


def before_feature(context, feature):
    kcli_plans_dir_path = os.path.join(
        os.getcwd(),
        KCLI_PLANS_DIR,
    )
    if not os.path.exists(kcli_plans_dir_path):
        os.mkdir(kcli_plans_dir_path)

    vm_description, ceph_description = has_ceph_configuration(
        feature.description,
        "- configure ceph cluster",
    )
    loaded_kcli, loaded_script = _loaded_templates()

    vm_feature_specs = " ".join(
        [line for line in vm_description if line.startswith("-")]
    )
    vm_config = _parse_vm_description("".join(vm_feature_specs))
    kcli_plan_path = os.path.join(kcli_plans_dir_path, "gen_kcli_plan.yml")
    print(f"Kcli vm configuration \n {vm_config}")
    _write_file(
        kcli_plan_path,
        loaded_kcli.render(vm_config)
    )

    # Checks for ceph description if None set the default configurations
    ceph_config = _parse_ceph_description(
        "".join(ceph_description)
    ) if ceph_description else Bootstrap_Config

    print(f"Bootstrap configuration \n {ceph_config}\n")
    _write_file(
        os.path.join(kcli_plans_dir_path, "bootstrap_cluster_dev.sh"),
        loaded_script.render(ceph_config),
    )

    _handle_kcli_plan("create", os.path.relpath(kcli_plan_path))

    if not is_bootstrap_script_complete():
        print("Failed to complete bootstrap..")
        _handle_kcli_plan("delete")
        exit(1)
    context.last_executed = {}


def after_feature(context, feature):
    if os.path.exists(KCLI_PLANS_DIR):
        _clean_generated(os.path.abspath(KCLI_PLANS_DIR))
    _handle_kcli_plan("delete")
