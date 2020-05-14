#!/usr/bin/python3

import sys, os
import json

def main(line):
    line = line[5:]
    
    # python3 ./gen_static_command_descriptions.py > ceph_commands.json
    with open('ceph_commands.json') as f:
        commands = json.load(f)

    sigs = [c["sig"] for c in commands]
    sigs_strs = [' '.join(elem for elem in sig if isinstance(elem, str)) for sig in sigs]
    print(' '.join(s for s in sigs_strs if s.startswith(line)))
    
    
    
main(os.environ["COMP_LINE"])
