from mgr_module import MgrModule, CommandResult, HandleCommandResult
import os
import json
import threading
import subprocess
from string import Template
from tempfile import mkstemp

class Module(MgrModule):

    MODULE_OPTIONS = [
        {
            'name': 'subtree',
            'type': 'str',
            'default': 'rack',
            'desc': 'CRUSH level for which to create a local pool',
            'runtime': True,
        },
        {
            'name': 'failure_domain',
            'type': 'str',
            'default': 'host',
            'desc': 'failure domain for any created local pool',
            'runtime': True,
        },
        {
            'name': 'min_size',
            'type': 'int',
            'desc': 'default min_size for any created local pool',
            'runtime': True,
        },
        {
            'name': 'num_rep',
            'type': 'int',
            'default': 3,
            'desc': 'default replica count for any created local pool',
            'runtime': True,
        },
        {
            'name': 'pg_num',
            'type': 'int',
            'default': 128,
            'desc': 'default pg_num for any created local pool',
            'runtime': True,
        },
        {
            'name': 'prefix',
            'type': 'str',
            'default': '',
            'desc': 'name prefix for any created local pool',
            'runtime': True,
        },
    ]

    COMMANDS = [
        {
            "cmd": "affinity create pool "
                   "name=poolname,type=CephString,req=true "
                   "name=region,type=CephString,req=true "
                   "name=pd,type=CephString,req=true "
                   "name=sd,type=CephString,req=true "
                   "name=td,type=CephString,req=true",
            "desc": "create a new pool with primary affinity to pd",
            "perm": "rw"
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.serve_event = threading.Event()

    def notify(self, notify_type, notify_id):
        if notify_type == 'osd_map':
            self.handle_osd_map()

    def get_highest_rule_id(self):
        crush = self.get('osd_map_crush')
        crush_rules = crush['rules']
        id = 1
        for rule in crush_rules:
            if rule['rule_id'] > id:
                id = rule['rule_id']

        return id
    def create_crush_affined_rule(self, name, region, pd, sd, td):
        rule_template = Template(""" rule ${name} {
    id ${rule_id}
    type replicated
    min_size 1
    max_size 10
    step take ${region}${primary_domain}
    step chooseleaf firstn 1 type host
    step emit
    step take ${region}${secondary_domain}
    step chooseleaf firstn 1 type host
    step emit
    step take ${region}${tertiary_domain}
    step chooseleaf firstn 1 type host
    step emit
}""")
        
        tempfd, crushmap_filename = mkstemp(dir='/tmp')

        editable_crushmap_filename = crushmap_filename+".editable"

        #get binary crush map
        ret, buf, errs = self.rados.mon_command(json.dumps({"prefix":"osd getcrushmap"}), b'')
        if ret != 0:
            raise RuntimeError("Error getting binary crushmap. ret {}: '{}'".format(
                ret, errs))

        prior_ver = int(errs)

        # write binary crush map to file for crushtool
        with open(crushmap_filename,"wb") as f:
            f.write(bytearray(buf))

        os.close(tempfd)

        # decompile binary crush map
        subprocess.check_call(["crushtool","-d",crushmap_filename,"-o",editable_crushmap_filename])

        # write new rule to editable crush map, using template
        with open(editable_crushmap_filename,"a") as cmfile:
            cmfile.write(rule_template.substitute(
                name=name, 
                rule_id=self.get_highest_rule_id()+1,
                region=region,
                primary_domain=pd,
                secondary_domain=sd,
                tertiary_domain=td
            ))

        # compile editable crush map
        subprocess.check_call(["crushtool","-c",editable_crushmap_filename,"-o",crushmap_filename])

        # read compiled, binary crush map
        with open(crushmap_filename,"rb") as f:
            barr = f.read()

        # apply binary crush map
        ret, buf, errs = self.rados.mon_command(json.dumps({
            "prefix":"osd setcrushmap", 
            "prior_version":prior_ver}), barr)
        if ret != 0:
            raise RuntimeError("Error setting binary crushmap. command {}: ret {}: '{}'".format(
                "osd setcrushmap prior_version=" + prior_ver, ret, errs))

        # remove temporary files
        os.remove(crushmap_filename)
        os.remove(editable_crushmap_filename)

    def handle_command(self, inbuf, cmd):
        self.log.debug("Handling command: '%s'" % str(cmd))
        message=cmd['prefix']
        message += "uid: " 
        message += str(os.getuid())

        if cmd['prefix'] == "affinity create pool":
            self.create_crush_affined_rule(
                    cmd['poolname'] + "rule",
                    cmd['region'],
                    cmd['pd'],
                    cmd['sd'],
                    cmd['td'])
            self.log.debug("after create rule")
            
        status_code = 0
        output_buffer = ""
        output_string = ""

        message=cmd['prefix']
        return HandleCommandResult(retval=status_code, stdout=output_buffer,
                                   stderr=message + "\n" + output_string)

    def handle_osd_map(self):
        """
        Check pools on each OSDMap change
        """
        subtree_type = self.get_module_option('subtree')
        failure_domain = self.get_module_option('failure_domain')
        pg_num = self.get_module_option('pg_num')
        num_rep = self.get_module_option('num_rep')
        min_size = self.get_module_option('min_size')
        prefix = self.get_module_option('prefix') or 'by-' + subtree_type + '-'

        osdmap = self.get("osd_map")
        lpools = []
        for pool in osdmap['pools']:
            if pool['pool_name'].find(prefix) == 0:
                lpools.append(pool['pool_name'])

        self.log.debug('localized pools = %s', lpools)
        subtrees = []
        tree = self.get('osd_map_tree')
        for node in tree['nodes']:
            if node['type'] == subtree_type:
                subtrees.append(node['name'])
                pool_name = prefix + node['name']
                if pool_name not in lpools:
                    self.log.info('Creating localized pool %s', pool_name)
                    #
                    result = CommandResult("")
                    self.send_command(result, "mon", "", json.dumps({
                        "prefix": "osd crush rule create-replicated",
                        "format": "json",
                        "name": pool_name,
                        "root": node['name'],
                        "type": failure_domain,
                    }), "")
                    r, outb, outs = result.wait()

                    result = CommandResult("")
                    self.send_command(result, "mon", "", json.dumps({
                        "prefix": "osd pool create",
                        "format": "json",
                        "pool": pool_name,
                        'rule': pool_name,
                        "pool_type": 'replicated',
                        'pg_num': pg_num,
                    }), "")
                    r, outb, outs = result.wait()

                    result = CommandResult("")
                    self.send_command(result, "mon", "", json.dumps({
                        "prefix": "osd pool set",
                        "format": "json",
                        "pool": pool_name,
                        'var': 'size',
                        "val": str(num_rep),
                    }), "")
                    r, outb, outs = result.wait()

                    if min_size:
                        result = CommandResult("")
                        self.send_command(result, "mon", "", json.dumps({
                            "prefix": "osd pool set",
                            "format": "json",
                            "pool": pool_name,
                            'var': 'min_size',
                            "val": str(min_size),
                        }), "")
                        r, outb, outs = result.wait()

        # TODO remove pools for hosts that don't exist?

    def serve(self):
        self.handle_osd_map()
        self.serve_event.wait()
        self.serve_event.clear()

        while self.run:
            sleep_interval = 5
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            ret = self.event.wait(sleep_interval)
            self.event.clear()

    def shutdown(self):
        self.serve_event.set()
