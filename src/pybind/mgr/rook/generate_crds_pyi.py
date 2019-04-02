"""
Generate Python Sub file containing static type information for checking
the CRRs submitted to Rook

Usage:

Run

$ python ./generate_crds_pyi.py > crds.pyi

to re-generate the stub file. And then run

$ cd ../../../
$ script/run_mypy.sh

to type-check the rook_cluster module.
"""
from collections import namedtuple

import yaml



ceph_cluster_crd = """
apiVersion: apiextensions.k8s.io/v1beta1
kind: CustomResourceDefinition
metadata:
  name: cephclusters.ceph.rook.io
spec:
  group: ceph.rook.io
  names:
    kind: CephCluster
    listKind: CephClusterList
    plural: cephclusters
    singular: cephcluster
  scope: Namespaced
  version: v1
  validation:
    openAPIV3Schema:
      properties:
        spec:
          properties:
            cephVersion:
              properties:
                allowUnsupported:
                  type: boolean
                image:
                  type: string
                name:
                  pattern: ^(luminous|mimic|nautilus)$
                  type: string
            dashboard:
              properties:
                enabled:
                  type: boolean
                urlPrefix:
                  type: string
                port:
                  type: integer
            dataDirHostPath:
              pattern: ^/(\S+)
              type: string
            mon:
              properties:
                allowMultiplePerNode:
                  type: boolean
                count:
                  maximum: 9
                  minimum: 1
                  type: integer
                preferredCount:
                  maximum: 9
                  minimum: 0
                  type: integer
              required:
              - count
            network:
              properties:
                hostNetwork:
                  type: boolean
            storage:
              properties:
                nodes:
                  items: {}
                  type: array
                useAllDevices: {}
                useAllNodes:
                  type: boolean
          required:
          - mon
"""


class PyiClass(namedtuple('PyiClass', ('name', 'attrs', 'parent'))):
    def __str__(self):
        return """class {name}({parent}):
{ps}""".format(name=self.name, ps='\n'.join(map(str, self.attrs)), parent=self.parent)


class PyiAttribute(namedtuple('PyiAttribute', ('name', 'type'))):
    def __str__(self):
        return '    {name}: {type}'.format(name=self.name, type=self.type)


def to_type_name(t):
    return {
        'integer': 'int',
        'boolean': 'bool',
        'string': 'str',
        'array': 'List',
    }[t]


def handle_property(sub_name, sub):
    if 'properties' in sub:
        return handle_obj(sub_name, sub)
    elif 'type' in sub:
        return PyiAttribute(sub_name, to_type_name(sub['type']))
    elif sub == {}:
        return PyiAttribute(sub_name, 'Any')
    assert False, str((sub_name, sub))


def handle_obj(name, o):
    ps = o['properties']
    return PyiClass(name, [handle_property(*i) for i in ps.items()], 'object')


def handle_crd(c_dict):
    name = c_dict['spec']['names']['kind']
    s = c_dict['spec']['validation']['openAPIV3Schema']
    c = handle_obj(name, s)
    k8s_attrs = [PyiAttribute('apiVersion', 'str'), PyiAttribute('kind', 'str'),
                 PyiAttribute('metadata', 'Any')]
    return PyiClass(c.name, k8s_attrs + c.attrs, 'CRD')


def flatten_classes(e):
    if isinstance(e, PyiClass):
        name, ps, parent = e
        ps, cs = zip(*[flatten_classes(p) for p in ps])
        cs = sum(list(cs), [])
        return PyiAttribute(name, to_class_name(name)), cs + [
            PyiClass(to_class_name(name), ps, parent)]
    elif isinstance(e, PyiAttribute):
        return e, []
    assert False, e


def to_class_name(name):
    return name[0].upper() + name[1:]


def main():
    crd_dict = yaml.safe_load(ceph_cluster_crd)
    tree = handle_crd(crd_dict)

    header = """# Warning.
# This file is generated. Do not touch.

from typing import List, Dict, Any


class CRD(object):
    def _to_dict(self) -> Dict:
        ...

"""

    print(header)
    res = '\n\n\n'.join(map(str, flatten_classes(tree)[1]))
    print(res)


if __name__ == '__main__':
    main()