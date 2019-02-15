  $ cp "$TESTDIR/choose-args.crush" .
  $ crushtool -c choose-args.crush -o choose-args.compiled
  $ crushtool -d choose-args.compiled -o choose-args.conf
  $ crushtool -c choose-args.conf -o choose-args.recompiled
  $ cmp choose-args.crush choose-args.conf
  $ cmp choose-args.compiled choose-args.recompiled
  $ crushtool -c choose-args.conf -o /dev/null --dump
  {
      "devices": [
          {
              "id": 0,
              "name": "device0"
          },
          {
              "id": 1,
              "name": "device1"
          },
          {
              "id": 2,
              "name": "device2"
          }
      ],
      "types": [
          {
              "type_id": 0,
              "name": "device"
          },
          {
              "type_id": 1,
              "name": "host"
          },
          {
              "type_id": 2,
              "name": "rack"
          },
          {
              "type_id": 3,
              "name": "root"
          }
      ],
      "buckets": [
          {
              "id": -1,
              "name": "host0",
              "type_id": 1,
              "type_name": "host",
              "weight": 65536,
              "alg": "straw2",
              "hash": "rjenkins1",
              "items": [
                  {
                      "id": 0,
                      "weight": 65536,
                      "pos": 0
                  }
              ]
          },
          {
              "id": -2,
              "name": "host1",
              "type_id": 1,
              "type_name": "host",
              "weight": 65536,
              "alg": "straw2",
              "hash": "rjenkins1",
              "items": [
                  {
                      "id": 1,
                      "weight": 65536,
                      "pos": 0
                  }
              ]
          },
          {
              "id": -3,
              "name": "rack0",
              "type_id": 2,
              "type_name": "rack",
              "weight": 196608,
              "alg": "straw2",
              "hash": "rjenkins1",
              "items": [
                  {
                      "id": -1,
                      "weight": 65536,
                      "pos": 0
                  },
                  {
                      "id": -2,
                      "weight": 65536,
                      "pos": 1
                  },
                  {
                      "id": -5,
                      "weight": 65536,
                      "pos": 2
                  }
              ]
          },
          {
              "id": -4,
              "name": "root",
              "type_id": 3,
              "type_name": "root",
              "weight": 262144,
              "alg": "straw2",
              "hash": "rjenkins1",
              "items": [
                  {
                      "id": -3,
                      "weight": 262144,
                      "pos": 0
                  }
              ]
          },
          {
              "id": -5,
              "name": "host2",
              "type_id": 1,
              "type_name": "host",
              "weight": 65536,
              "alg": "straw2",
              "hash": "rjenkins1",
              "items": [
                  {
                      "id": 2,
                      "weight": 65536,
                      "pos": 0
                  }
              ]
          }
      ],
      "rules": [
          {
              "rule_id": 3,
              "rule_name": "data",
              "ruleset": 3,
              "type": 1,
              "min_size": 2,
              "max_size": 2,
              "steps": [
                  {
                      "op": "take",
                      "item": -4,
                      "item_name": "root"
                  },
                  {
                      "op": "chooseleaf_firstn",
                      "num": 0,
                      "type": "rack"
                  },
                  {
                      "op": "emit"
                  }
              ]
          }
      ],
      "tunables": {
          "choose_local_tries": 2,
          "choose_local_fallback_tries": 5,
          "choose_total_tries": 19,
          "chooseleaf_descend_once": 0,
          "chooseleaf_vary_r": 0,
          "chooseleaf_stable": 0,
          "straw_calc_version": 0,
          "allowed_bucket_algs": 22,
          "profile": "argonaut",
          "optimal_tunables": 0,
          "legacy_tunables": 1,
          "minimum_required_version": "hammer",
          "require_feature_tunables": 0,
          "require_feature_tunables2": 0,
          "has_v2_rules": 0,
          "require_feature_tunables3": 0,
          "has_v3_rules": 0,
          "has_v4_buckets": 1,
          "require_feature_tunables5": 0,
          "has_v5_rules": 0
      },
      "choose_args": {
          "1": [],
          "2": [
              {
                  "bucket_id": -3,
                  "ids": [
                      -20,
                      30,
                      -25
                  ]
              }
          ],
          "3": [
              {
                  "bucket_id": -3,
                  "weight_set": [
                      [
                          1,
                          2,
                          5
                      ],
                      [
                          3,
                          2,
                          5
                      ]
                  ],
                  "ids": [
                      -20,
                      -30,
                      -25
                  ]
              }
          ],
          "4": [
              {
                  "bucket_id": -2,
                  "weight_set": [
                      [
                          1
                      ],
                      [
                          3
                      ]
                  ]
              }
          ],
          "5": [
              {
                  "bucket_id": -1,
                  "ids": [
                      -450
                  ]
              }
          ],
          "6": [
              {
                  "bucket_id": -1,
                  "ids": [
                      -450
                  ]
              },
              {
                  "bucket_id": -2,
                  "weight_set": [
                      [
                          1
                      ],
                      [
                          3
                      ]
                  ]
              },
              {
                  "bucket_id": -3,
                  "weight_set": [
                      [
                          1,
                          2,
                          5
                      ],
                      [
                          3,
                          2,
                          5
                      ]
                  ],
                  "ids": [
                      -20,
                      -30,
                      -25
                  ]
              }
          ]
      }
  }
  

  $ crushtool -c choose-args.conf -o /dev/null --dump | jq .for_json_validation
  null
