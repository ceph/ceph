class GaneshaConfParser:
    def __init__(self, raw_config):
        self.pos = 0
        self.text = ""
        self.clean_config(raw_config)

    def clean_config(self, raw_config):
        for line in raw_config.split("\n"):
            self.text += line
            if line.startswith("%"):
                self.text += "\n"

    def remove_whitespaces_quotes(self):
        if self.text.startswith("%url"):
            self.text = self.text.replace('"', "")
        else:
            self.text = "".join(self.text.split())

    def stream(self):
        return self.text[self.pos:]

    def parse_block_name(self):
        idx = self.stream().find('{')
        if idx == -1:
            raise Exception("Cannot find block name")
        block_name = self.stream()[:idx]
        self.pos += idx+1
        return block_name

    def parse_block_or_section(self):
        if self.stream().startswith("%url "):
            # section line
            self.pos += 5
            idx = self.stream().find('\n')
            if idx == -1:
                value = self.stream()
                self.pos += len(value)
            else:
                value = self.stream()[:idx]
                self.pos += idx+1
            block_dict = {'block_name': '%url', 'value': value}
            return block_dict

        block_dict = {'block_name': self.parse_block_name().upper()}
        self.parse_block_body(block_dict)
        if self.stream()[0] != '}':
            raise Exception("No closing bracket '}' found at the end of block")
        self.pos += 1
        return block_dict

    def parse_parameter_value(self, raw_value):
        if raw_value.find(',') != -1:
            return [self.parse_parameter_value(v.strip())
                    for v in raw_value.split(',')]
        try:
            return int(raw_value)
        except ValueError:
            if raw_value == "true":
                return True
            if raw_value == "false":
                return False
            if raw_value.find('"') == 0:
                return raw_value[1:-1]
            return raw_value

    def parse_stanza(self, block_dict):
        equal_idx = self.stream().find('=')
        if equal_idx == -1:
            raise Exception("Malformed stanza: no equal symbol found.")
        semicolon_idx = self.stream().find(';')
        parameter_name = self.stream()[:equal_idx].lower()
        parameter_value = self.stream()[equal_idx+1:semicolon_idx]
        block_dict[parameter_name] = self.parse_parameter_value(parameter_value)
        self.pos += semicolon_idx+1

    def parse_block_body(self, block_dict):
        while True:
            if self.stream().find('}') == 0:
                # block end
                return

            last_pos = self.pos
            semicolon_idx = self.stream().find(';')
            lbracket_idx = self.stream().find('{')
            is_semicolon = (semicolon_idx != -1)
            is_lbracket = (lbracket_idx != -1)
            is_semicolon_lt_lbracket = (semicolon_idx < lbracket_idx)

            if is_semicolon and ((is_lbracket and is_semicolon_lt_lbracket) or not is_lbracket):
                self.parse_stanza(block_dict)
            elif is_lbracket and ((is_semicolon and not is_semicolon_lt_lbracket) or
                                  (not is_semicolon)):
                if '_blocks_' not in block_dict:
                    block_dict['_blocks_'] = []
                block_dict['_blocks_'].append(self.parse_block_or_section())
            else:
                raise Exception("Malformed stanza: no semicolon found.")

            if last_pos == self.pos:
                raise Exception("Infinite loop while parsing block content")

    def parse(self):
        self.remove_whitespaces_quotes()
        blocks = []
        while self.stream():
            blocks.append(self.parse_block_or_section())
        return blocks

    @staticmethod
    def _indentation(depth, size=4):
        conf_str = ""
        for _ in range(0, depth*size):
            conf_str += " "
        return conf_str

    @staticmethod
    def write_block_body(block, depth=0):
        def format_val(key, val):
            if isinstance(val, list):
                return ', '.join([format_val(key, v) for v in val])
            if isinstance(val, bool):
                return str(val).lower()
            if isinstance(val, int) or (block['block_name'] == 'CLIENT'
                                        and key == 'clients'):
                return '{}'.format(val)
            return '"{}"'.format(val)

        conf_str = ""
        for key, val in block.items():
            if key == 'block_name':
                continue
            elif key == '_blocks_':
                for blo in val:
                    conf_str += GaneshaConfParser.write_block(blo, depth)
            elif val:
                conf_str += GaneshaConfParser._indentation(depth)
                conf_str += '{} = {};\n'.format(key, format_val(key, val))
        return conf_str

    @staticmethod
    def write_block(block, depth=0):
        if block['block_name'] == "%url":
            return '%url "{}"\n\n'.format(block['value'])

        conf_str = ""
        conf_str += GaneshaConfParser._indentation(depth)
        conf_str += format(block['block_name'])
        conf_str += " {\n"
        conf_str += GaneshaConfParser.write_block_body(block, depth+1)
        conf_str += GaneshaConfParser._indentation(depth)
        conf_str += "}\n"
        return conf_str


class CephFSFSal:
    def __init__(self, name, user_id=None, fs_name=None, sec_label_xattr=None,
                 cephx_key=None):
        self.name = name
        self.fs_name = fs_name
        self.user_id = user_id
        self.sec_label_xattr = sec_label_xattr
        self.cephx_key = cephx_key

    @classmethod
    def from_fsal_block(cls, fsal_block):
        return cls(fsal_block['name'],
                   fsal_block.get('user_id', None),
                   fsal_block.get('filesystem', None),
                   fsal_block.get('sec_label_xattr', None),
                   fsal_block.get('secret_access_key', None))

    def to_fsal_block(self):
        result = {
            'block_name': 'FSAL',
            'name': self.name,
        }
        if self.user_id:
            result['user_id'] = self.user_id
        if self.fs_name:
            result['filesystem'] = self.fs_name
        if self.sec_label_xattr:
            result['sec_label_xattr'] = self.sec_label_xattr
        if self.cephx_key:
            result['secret_access_key'] = self.cephx_key
        return result

    @classmethod
    def from_dict(cls, fsal_dict):
        return cls(fsal_dict['name'], fsal_dict['user_id'],
                   fsal_dict['fs_name'], fsal_dict['sec_label_xattr'], None)

    def to_dict(self):
        return {
            'name': self.name,
            'user_id': self.user_id,
            'fs_name': self.fs_name,
            'sec_label_xattr': self.sec_label_xattr
        }


class Client:
    def __init__(self, addresses, access_type=None, squash=None):
        self.addresses = addresses
        self.access_type = access_type
        self.squash = squash

    @classmethod
    def from_client_block(cls, client_block):
        addresses = client_block['clients']
        if not isinstance(addresses, list):
            addresses = [addresses]
        return cls(addresses,
                   client_block.get('access_type', None),
                   client_block.get('squash', None))

    def to_client_block(self):
        result = {
            'block_name': 'CLIENT',
            'clients': self.addresses,
        }
        if self.access_type:
            result['access_type'] = self.access_type
        if self.squash:
            result['squash'] = self.squash
        return result

    @classmethod
    def from_dict(cls, client_dict):
        return cls(client_dict['addresses'], client_dict['access_type'],
                   client_dict['squash'])

    def to_dict(self):
        return {
            'addresses': self.addresses,
            'access_type': self.access_type,
            'squash': self.squash
        }


class Export:
    def __init__(self, export_id, path, cluster_id, pseudo, access_type, squash, security_label,
            protocols, transports, fsal, clients=None):
        self.export_id = export_id
        self.path = path
        self.fsal = fsal
        self.cluster_id = cluster_id
        self.pseudo = pseudo
        self.access_type = access_type
        self.squash = squash
        self.attr_expiration_time = 0
        self.security_label = security_label
        self.protocols = protocols
        self.transports = transports
        self.clients = clients

    @classmethod
    def from_export_block(cls, export_block, cluster_id):
        fsal_block = [b for b in export_block['_blocks_']
                      if b['block_name'] == "FSAL"]

        client_blocks = [b for b in export_block['_blocks_']
                         if b['block_name'] == "CLIENT"]

        protocols = export_block.get('protocols')
        if not isinstance(protocols, list):
            protocols = [protocols]

        transports = export_block.get('transports')
        if not isinstance(transports, list):
            transports = [transports]

        return cls(export_block['export_id'],
                   export_block['path'],
                   cluster_id,
                   export_block['pseudo'],
                   export_block['access_type'],
                   export_block['squash'],
                   export_block['security_label'],
                   protocols,
                   transports,
                   CephFSFSal.from_fsal_block(fsal_block[0]),
                   [Client.from_client_block(client)
                    for client in client_blocks])

    def to_export_block(self):
        result = {
            'block_name': 'EXPORT',
            'export_id': self.export_id,
            'path': self.path,
            'pseudo': self.pseudo,
            'access_type': self.access_type,
            'squash': self.squash,
            'attr_expiration_time': self.attr_expiration_time,
            'security_label': self.security_label,
            'protocols': self.protocols,
            'transports': self.transports,
        }
        result['_blocks_'] = [self.fsal.to_fsal_block()]
        result['_blocks_'].extend([client.to_client_block()
                                   for client in self.clients])
        return result

    @classmethod
    def from_dict(cls, export_id, ex_dict):
        return cls(export_id,
                   ex_dict['path'],
                   ex_dict['cluster_id'],
                   ex_dict['pseudo'],
                   ex_dict.get('access_type', 'R'),
                   ex_dict.get('squash', 'no_root_squash'),
                   ex_dict.get('security_label', True),
                   ex_dict.get('protocols', [4]),
                   ex_dict.get('transports', ['TCP']),
                   CephFSFSal.from_dict(ex_dict['fsal']),
                   [Client.from_dict(client) for client in ex_dict['clients']])

    def to_dict(self):
        return {
            'export_id': self.export_id,
            'path': self.path,
            'cluster_id': self.cluster_id,
            'pseudo': self.pseudo,
            'access_type': self.access_type,
            'squash': self.squash,
            'security_label': self.security_label,
            'protocols': sorted([p for p in self.protocols]),
            'transports': sorted([t for t in self.transports]),
            'fsal': self.fsal.to_dict(),
            'clients': [client.to_dict() for client in self.clients]
        }
