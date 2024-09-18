import yaml

# Step 1: Load the YAML file
with open('hosts.yaml', 'r') as yaml_file:
    yaml_data = yaml.safe_load(yaml_file)

# Step 2: Create a function to generate the Ceph Orchestrator commands
def generate_ceph_commands(hosts, services):
    commands = []
    
    base_services = ['mon', 'mgr']
    for service in base_services:
        commands.append(f"ceph orch apply {service} --unmanaged")

    for host in hosts:
        ip = host['ipaddresses']
        labels = host['label']
        commands.append(f"ceph orch add host {ip} --labels {labels}")

    if 'monitor' in services:
        monitor = services['monitor']
        count_per_host = monitor.get('count-per-host', 1)
        commands.append(f'ceph orch apply mon --placement="label:mon count-per-host:{count_per_host}"')
    else:
        commands.append(f'ceph orch apply mon --placement="label:mon count-per-host:1"')

    if 'manager' in services:
        manager = services['manager']
        count_per_host = manager.get('count-per-host', 1)
        commands.append(f'ceph orch apply mgr --placement="label:mgr count-per-host:{count_per_host}"')

    if 'radosgw' in services:
        radosgw_list = services['radosgw']
        for rgw_service in radosgw_list:
            service_name = rgw_service.get('name', 'default')
            port = rgw_service.get('port', 8080)  # Default port if none is provided
            count_per_host = rgw_service.get('count-per-host', 1)
            commands.append(f"ceph orch apply rgw {service_name} '--placement=label:rgw count-per-host:{count_per_host}' --port={port}")

    return commands

# Step 3: Extract data from YAML and generate the commands
hosts = yaml_data['hosts']
services = yaml_data.get('services', {})

commands = generate_ceph_commands(hosts, services)

# Step 4: Write the commands to a .sh file
with open('output.sh', 'w') as output_file:
    output_file.write("#!/bin/bash\n\n")
    for command in commands:
        output_file.write(f"{command}\n")
