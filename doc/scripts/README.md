Script Usage
============

Peering State Model: gen_state_diagram.py
------------------------------------------
    $ git clone https://github.com/ceph/ceph.git
    $ cd ceph
    $ cat src/osd/PeeringState.h src/osd/PeeringState.cc | doc/scripts/gen_state_diagram.py > doc/dev/peering_graph.generated.dot
    $ sed -i 's/7,7/1080,1080/' doc/dev/peering_graph.generated.dot
    $ dot -Tsvg doc/dev/peering_graph.generated.dot > doc/dev/peering_graph.generated.svg

