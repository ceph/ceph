
osd_header = """
{:-^80}""".format('')


osd_component_titles = """
  Type            Path                      LV Size         % of device"""


osd_component = """
  {_type: <15} {path: <25} {size: <15} {percent}%"""


total_osds = """
Total OSDs: {total_osds}
"""

ssd_volume_group = """
Solid State VG:
  Targets:   {target: <25} Total size: {total_lv_size: <25}
  Total LVs: {total_lvs: <25} Size per LV: {lv_size: <25}
  Devices:   {block_db_devices}
"""


