
osd_header = """
{:-^100}""".format('')


osd_component_titles = """
  Type            Path                                                    LV Size         % of device"""


osd_reused_id = """
  OSD id          {id_: <55}"""


osd_component = """
  {_type: <15} {path: <55} {size: <15} {percent:.2%}"""


osd_encryption = """
  encryption:     {enc: <15}"""


total_osds = """
Total OSDs: {total_osds}
"""


def filtered_devices(devices):
    string = """
Filtered Devices:"""
    for device, info in devices.items():
        string += """
  %s""" % device

        for reason in info['reasons']:
            string += """
    %s""" % reason

    string += "\n"
    return string


ssd_volume_group = """
Solid State VG:
  Targets:   {target: <25} Total size: {total_lv_size: <25}
  Total LVs: {total_lvs: <25} Size per LV: {lv_size: <25}
  Devices:   {block_db_devices}
"""


