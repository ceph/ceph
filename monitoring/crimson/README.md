# Crimson OSD dashboard

Prometheus + Grafana for the crimson OSD metrics endpoint
(`crimson_prometheus_port_base`, see `doc/crimson/crimson.rst`).

| File | Purpose |
|---|---|
| `gen_dashboard.py` | Generates `crimson-osd.json`. Change the dashboard here, not in the JSON. |
| `crimson-osd.json` | The Grafana dashboard. |
| `vstart-monitoring.sh` | Runs Prometheus and Grafana containers for a vstart cluster. |

## Requirements 

The following needs to be available to run the monitoring script: 
* `podman` or `docker` 
* `curl`  
* `python3`

Also, the `vstart` cluster running with crimson OSDs, must have configured the `crimson_prometheus_port_base`, eg: 

```
ceph config set osd crimson_prometheus_port_base 9400
```


## Running the script 

```
../monitoring/crimson/vstart-monitoring.sh start
```

This will do the following: 
- Start Grafana at http://127.0.0.1 :3000. Anonymous users are viewers and you can log in as `admin` user with password `admin` to edit.
- Prometheus server is started at port 9090. (If 9090 is busy, it selects the next available port automatically.)


To view the dashboard in your local machine, do the following: 

```
ssh -L 3000:localhost:3000 <vstart host>`
```

## Other Commands 

```bash
../monitoring/crimson/vstart-monitoring.sh status     # containers, URLs and target health
../monitoring/crimson/vstart-monitoring.sh targets    # after OSDs are added or removed
../monitoring/crimson/vstart-monitoring.sh stop       # remove containers, keep data
../monitoring/crimson/vstart-monitoring.sh purge      # remove containers and data
```

`../src/stop.sh` (with or without `--crimson`) also removes the containers when
it stops the whole cluster. The data is preserved between runs, unless intentionally purged. 

## Change the dashboard

To modify the dashboard, make changed to the `gen_dashboard.py` and generate the new JSON file with the following command:  

```bash
python3 monitoring/crimson/gen_dashboard.py
../monitoring/crimson/vstart-monitoring.sh dashboard   # from the build dir; Grafana reloads it
```


