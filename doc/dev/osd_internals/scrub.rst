
Scrubbing Behavior Table
========================

+-------------------------------------------------+----------+-----------+---------------+----------------------+
|                                          Flags  | none     | noscrub   | nodeep_scrub  | noscrub/nodeep_scrub |
+=================================================+==========+===========+===============+======================+
| Periodic tick                                   |   S      |    X      |     S         |         X            |
+-------------------------------------------------+----------+-----------+---------------+----------------------+
| Periodic tick after osd_deep_scrub_interval     |   D      |    D      |     S         |         X            |
+-------------------------------------------------+----------+-----------+---------------+----------------------+
| Initiated scrub                                 |   S      |    S      |     S         |         S            |
+-------------------------------------------------+----------+-----------+---------------+----------------------+
| Initiated scrub after osd_deep_scrub_interval   |   D      |    D      |     S         |         S            |
+-------------------------------------------------+----------+-----------+---------------+----------------------+
| Initiated deep scrub                            |   D      |    D      |     D         |         D            |
+-------------------------------------------------+----------+-----------+---------------+----------------------+

- X = Do nothing
- S = Do regular scrub
- D = Do deep scrub

State variables
---------------

- Periodic tick state is !must_scrub && !must_deep_scrub && !time_for_deep 
- Periodic tick after osd_deep_scrub_interval state is !must_scrub && !must_deep_scrub && time_for_deep 
- Initiated scrub state is  must_scrub && !must_deep_scrub && !time_for_deep
- Initiated scrub after osd_deep_scrub_interval state is must scrub && !must_deep_scrub && time_for_deep
- Initiated deep scrub state is  must_scrub && must_deep_scrub
