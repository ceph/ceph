import json

# Sample JSON data (as a string for demonstration)
json_data = '''
[
    {
        "addr": "171.254.95.156",
        "hostname": "ceph-node-2",
        "labels": [
            "_admin",
            "mgr",
            "mon",
            "osd"
        ],
        "status": ""
    },
    {
        "addr": "171.254.95.193",
        "hostname": "ceph-node-1",
        "labels": [
            "osd"
        ],
        "status": ""
    },
    {
        "addr": "171.254.93.217",
        "hostname": "ceph-node-3",
        "labels": [
            "osd"
        ],
        "status": ""
    }
]
'''

# Load the JSON data
current_labels_list = json.loads(json_data)

# Function to get labels for a specific hostname
def get_labels_for_hostname(hostname, current_labels):
    for host in current_labels_list:
        if host['hostname'] == hostname:
            return host['labels']  
    return [] 

# Example usage
hostnames = ["ceph-node-2", "ceph-node-1", "ceph-node-3"]
for hostname in hostnames:
    labels = get_labels_for_hostname(hostname, current_labels_list)
    print(f"Labels for {hostname}: {labels}")
