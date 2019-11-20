# Activate module
You can activate the Ceph Manager module by running:
```
$ ceph mgr module enable test_orchestrator
$ ceph orchestrator set backend test_orchestrator
```

# Check status
```
ceph orchestrator status
```

# Import dummy data
```
$ ceph test_orchestrator load_data -i ./dummy_data.json
```
