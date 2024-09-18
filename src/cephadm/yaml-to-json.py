import yaml
import json

def translate_yaml_to_json(yaml_file):
    # Load YAML file
    with open(yaml_file, 'r') as f:
        yaml_data = yaml.safe_load(f)

    # Directly create the JSON structure with compact mapping
    return {
        "_args": {
            key: yaml_data.get(key, default)
            for key, default in {
                "image": None,
                "docker": False,
                "fsid": "0f907084-70af-11ef-9fe4-7d9b10252be2",
                "data_dir": "/var/lib/ceph",
                "log_dir": "/var/log/ceph",
                "logrotate_dir": "/etc/logrotate.d",
                "sysctl_dir": "/etc/sysctl.d",
                "unit_dir": "/etc/systemd/system",
                "verbose": False,
                "log_dest": None,
                "timeout": None,
                "retry": 15,
                "env": [],
                "no_container_init": False,
                "no_cgroups_split": False,
                "output_dir": "/etc/ceph",
                "output_keyring": None,
                "output_config": None,
                "output_pub_ssh_key": None,
                "skip_admin_label": False,
                "skip_ssh": False,
                "initial_dashboard_user": "admin",
                "initial_dashboard_password": "passowrd",
                "ssl_dashboard_port": 8848,
                "ssh_user": "root",
                "skip_mon_network": False,
                "skip_dashboard": False,
                "dashboard_password_noupdate": False,
                "no_minimize_config": False,
                "skip_ping_check": False,
                "skip_pull": False,
                "skip_firewalld": False,
                "allow_overwrite": False,
                "no_cleanup_on_failure": False,
                "allow_fqdn_hostname": False,
                "allow_mismatched_release": False,
                "skip_prepare_host": False,
                "orphan_initial_daemons": False,
                "skip_monitoring_stack": True,
                "with_centralized_logging": False,
                "apply_spec": None,
                "shared_ceph_folder": None,
                "registry_url": None,
                "registry_username": None,
                "registry_password": None,
                "registry_json": None,
                "container_init": True,
                "cluster_network": None,
                "single_host_defaults": False,
                "log_to_file": False,
                "deploy_cephadm_agent": False,
                "custom_prometheus_alerts": None,
                "force": True,
                "keep_logs": False,
                "zap_osds": True,
                "name": None,
                "keyring": None,
                "mount": None,
                "command": [
                    "execute.sh"
                ],
                "funcs": [
                    "command_prepare_host",
                    "command_rm_cluster",
                    "command_bootstrap",
                    "command_shell"
                ]
            }.items()
        },
        "_conf": "<cephadmlib.context.BaseConfig object>"
    }

yaml_file = 'testing.yaml'
json_result = translate_yaml_to_json(yaml_file)
folder = 'context-logs'
context_filename = f'{folder}/context-TEST.json'
with open(context_filename, 'w') as f:
    f.write(json.dumps(json_result, indent=4, default=str))