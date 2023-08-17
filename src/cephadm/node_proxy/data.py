
system_1 = {

    'metadata': {
        'name': 'xx',
        'manufacturer': 'Dell',
        'model': 'HP PowerEdge',
        'chassis': 'xxx',
        'xxx': '',
    },

    'status': {
        'State': 'Enabled',
        'Health': 'OK'
    },

    'processors': [{
        'description': '',
        'cores': '',
        'threads': '',
        'type': '',
        'model': '',
        'temperature': '',
        'status': {
            'State': 'Enabled',
            'Health': 'OK'
        }
    }],

    'memory': {
        'description': '',
        'total': 'xx',
        'status': {
            'State': 'Enabled',
            'Health': 'OK'
        },
    },

    'network': {
        'interfaces': [
            {
                'type': 'ethernet',
                'description': 'my ethertnet interface',
                'name': 'name of the interface',
                'description': 'description of the interface',
                'speed_mbps': 'xxx',
                'status': {
                    'State': 'Enabled',
                    'Health': 'OK'
                },
            }
        ]
    },

    'storage': {
        'drives': [
            {
                'device': 'devc',
                'description': 'Milk, Cheese, Bread, Fruit, Vegetables',
                'serial_number': 'xxxxx',
                'location': '1I:x:y',
                'interface_type': 'SATA',
                'model': 'Buy groceries',
                'type': 'ssd|rotate|nvme',
                'capacity_bytes': '',
                'usage_bytes': '',
                'status': {
                    'State': 'Enabled',
                    'Health': 'OK'
                },
            }
        ]
    },

    'power':
        [{
            'type': 'xx',
            'manufacturer': 'xxx',
            'model': 'xx',
            'properties': {},
            'power_control': 'xx',
            'status': {
                'State': 'Enabled',
                'Health': 'OK'
            }
        }],

    'thermal': {
        'fans': [
            {
                'id': 1,
                'status': {
                    'State': 'Enabled',
                    'Health': 'OK'
                }
            }
        ]
    },
}
