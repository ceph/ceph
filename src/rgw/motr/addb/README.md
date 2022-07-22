ADDB is Motr sub-system, which provides convenient way for system events logging. ADDB is integrated and used by rgw_sal_motr.cc, while other RGW related code is not affected. Using ADDB allows to map S3 requests to subsequent Motr calls, that can be used for analysis and debugging purposes.
rgw_sal_motr.cc is instumented with ADDB() calls, which add entries into so-called ADDB storage object (ADDB stob). Entries are added in binary format and can be parsed to human-readable format by invoking `m0addb2dump` utility and RGW ADDB plugin, that allows to map binary entries to corresponding layer ID and parameters.
`m0addb2utility` can found after standard installation of cortx-motr rpm, and RGW ADDB plugin (rgw_addb_plugin.so) is installed during cortx-rgw-integration rpm installation to /opt/seagate/cortx/rgw/bin path.
RGW ADDB plugin can be compiled manually:

```console
# git clone https://github.com/Seagate/cortx-rgw-integration
# cd cortx-rgw-integration
# cd src/addb_plugin
# make plugin
```

Standard usage of RGW ADDB assumes its automatic conversion using PerfLine utility (Seagte CORTX utility for automatic performance runs).
ADDB stob can be converted manually by execution of the following command:
`# m0addb2dump -f -p /opt/seagate/cortx/rgw/bin/rgw_addb_plugin.so -- addb_12345/o/100000000000000:2 > dumpr_1.txt`

cortx-motr repo: https://github.com/Seagate/cortx-motr
cortx-rgw-integration repo: https://github.com/Seagate/cortx-rgw-integration