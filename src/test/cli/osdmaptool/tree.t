  $ osdmaptool --createsimple 3 om --with-default-pool
  osdmaptool: osdmap file 'om'
  osdmaptool: writing epoch 1 to om

  $ osdmaptool --tree=plain om
  osdmaptool: osdmap file 'om'
  ID CLASS WEIGHT  TYPE NAME              STATUS REWEIGHT PRI-AFF 
  -1       3.00000 root default                                   
  -3       3.00000     rack localrack                             
  -2       3.00000         host localhost                         
   0       1.00000             osd.0         DNE        0         
   1       1.00000             osd.1         DNE        0         
   2       1.00000             osd.2         DNE        0         

  $ osdmaptool --tree=json-pretty om
  osdmaptool: osdmap file 'om'
  {
      "nodes": [
          {
              "id": -1,
              "name": "default",
              "type": "root",
              "type_id": 11,
              "children": [
                  -3
              ]
          },
          {
              "id": -3,
              "name": "localrack",
              "type": "rack",
              "type_id": 3,
              "pool_weights": {},
              "children": [
                  -2
              ]
          },
          {
              "id": -2,
              "name": "localhost",
              "type": "host",
              "type_id": 1,
              "pool_weights": {},
              "children": [
                  2,
                  1,
                  0
              ]
          },
          {
              "id": 0,
              "name": "osd.0",
              "type": "osd",
              "type_id": 0,
              "crush_weight": 1,
              "depth": 3,
              "pool_weights": {},
              "exists": 0,
              "status": "down",
              "reweight": 0,
              "primary_affinity": 1
          },
          {
              "id": 1,
              "name": "osd.1",
              "type": "osd",
              "type_id": 0,
              "crush_weight": 1,
              "depth": 3,
              "pool_weights": {},
              "exists": 0,
              "status": "down",
              "reweight": 0,
              "primary_affinity": 1
          },
          {
              "id": 2,
              "name": "osd.2",
              "type": "osd",
              "type_id": 0,
              "crush_weight": 1,
              "depth": 3,
              "pool_weights": {},
              "exists": 0,
              "status": "down",
              "reweight": 0,
              "primary_affinity": 1
          }
      ],
      "stray": []
  }
  

  $ rm -f om

