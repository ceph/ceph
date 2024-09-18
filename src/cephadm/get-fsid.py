def get_fsid_from_conf(conf_file='/etc/ceph/ceph.conf'):
    print(conf_file)
    try:
        with open(conf_file, 'r') as file:
            for line in file:
                print(line)
                # Strip leading/trailing whitespace before checking
                if line.strip().startswith('fsid'):
                    return line.split('=')[1].strip()
    except FileNotFoundError:
        print(f"{conf_file} not found.")
        return None

print(get_fsid_from_conf())