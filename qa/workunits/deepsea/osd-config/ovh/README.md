These files contain all the permuations for possible OSD configrations.

The files were generated according to the following matrix.


```
+-------------+        +------------+         +------------------+      +-------------------------+
|             |        |            |         |                  |      |                         |
|             |        |            |         |                  |      |                         |
|             |        |   db       |         |  encrypted       |      | all have sizes          |
|   bluestore +-------->   wal      +--------->  not encrypted   +------> mixed size definition   |
|             |        |   db_wal   |         |                  |      | none have size defined  |
|             |        |            |         |                  |      |                         |
|             |        |            |         |                  |      |                         |
+-------------+        +------------+         +------------------+      +-------------------------+


+-------------+        +------------+        +-----------------+
|             |        |            |        |                 |
|  filestore  +--------> journal    +-------->  encrypted      |
|             |        |            |        |  not encrypted  |
|             |        |            |        |                 |
+-------------+        +------------+        +-----------------+
```
