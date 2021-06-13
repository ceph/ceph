
import os
import re

from jinja2 import Template

KCLI_PLANS_DIR = "kcli_plans"

KCLI_PLAN_TEMPLATE = """
parameters:
 nodes: {{ nodes }}
 pool: default
 network: default
 domain: cephlab.com
 prefix: ceph
 numcpus: {{ numcpus }}
 memory: {{ memory }}
 image: {{ image }}
 notify: false
 admin_password: password
 disks: {{ disks }}

{% raw %}
{% for number in range(0, nodes) %}
{{ prefix }}-node-0{{ number }}:
 image: {{ image }}
 numcpus: {{ numcpus }}
 memory: {{ memory }}
 reserveip: true
 reservedns: true
 sharedkey: true
 domain: {{ domain }}
 nets:
  - {{ network }}
 disks: {{ disks }}
 pool: {{ pool }}
 {% if ceph_dev_folder is defined %}
 sharedfolders: [{{ ceph_dev_folder }}]
 {% endif %}
 cmds:
 - yum -y install python3 chrony lvm2 podman
 - sed -i "s/SELINUX=enforcing/SELINUX=permissive/" /etc/selinux/config
 - echo "after installing the python3"
 - setenforce 0
 {% if number == 0 %}
 scripts:
  - bootstrap-cluster_dev.sh
 {% endif %}
{% endfor %}
{% endraw %}
"""

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
    "disks": [150, 3]
}


def _parse_memory_value(value):
    if value.endswith("gb"):
        value = value.replace('gb', '')
        return int(value) * 1024
    elif value.endswith("mb"):
        return value.replace('mb', '')


def _parse_value(spec_value):
    if spec_value.isnumeric():
        return int(spec_value)
    if spec_value.endswith("gb") or spec_value.endswith("mb"):
        return _parse_memory_value(spec_value)
    return spec_value


def _parse_feature_specifications(features):
    parsed_string = re.search(
        r"(?P<nodes>[\d]+) nodes with (?P<memory>[\w\.-]+) ram",
        features.lower(),
    )
    if parsed_string:
        for spec_key in parsed_string.groupdict().keys():
            Configuration[spec_key] = _parse_value(
                parsed_string.group(spec_key)
            )

    parsed_string = re.search(
        r"(?P<numcpus>[\d]+) cpus",
        features.lower(),
    )
    if parsed_string:
        Configuration["numcpus"] = parsed_string.group("numcpus")

    parsed_string = re.search(
        r"(?P<disk>[\d]+) storage devices of (?P<volume>[\w\.-]+)Gb each",
        features,
    )
    if parsed_string:
        Configuration["disks"] = [
            _parse_value(parsed_string.group("volume"))
            ] * _parse_value(parsed_string.group("disk"))

    parsed_string = re.search(
        r"(?P<image>[\w\.-]+) image",
        features.lower(),
    )
    if parsed_string:
        Configuration["image"] = parsed_string.group("image")

    return Configuration


def before_feature(context, feature):
    # Passing only line with specs
    kcli_plans_dir_path = os.path.join(os.getcwd(), KCLI_PLANS_DIR)
    if not os.path.exists(kcli_plans_dir_path):
        os.mkdir(kcli_plans_dir_path)

    loaded_template = Template(KCLI_PLAN_TEMPLATE)
    features = " ".join(
        [line for line in feature.description if line.startswith('-')]
    )
    feature_config = _parse_feature_specifications(
        "".join(features)
    )
    plans_file_path = os.path.join(kcli_plans_dir_path, "gen_kcli_plan.yml")
    generated_template = loaded_template.render(feature_config)
    with open(plans_file_path, "w") as file:
        file.write(generated_template)
