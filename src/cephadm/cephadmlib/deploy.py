# deploy.py - fundamental deployment types

from enum import Enum


class DeploymentType(Enum):
    # Fresh deployment of a daemon.
    DEFAULT = 'Deploy'
    # Redeploying a daemon. Works the same as fresh
    # deployment minus port checking.
    REDEPLOY = 'Redeploy'
    # Reconfiguring a daemon. Rewrites config
    # files and potentially restarts daemon.
    RECONFIG = 'Reconfig'
    # Staging a redeploy of a running daemon: config, keyring and the new
    # unit files are written next to the live ones (unit.*.staged) but the
    # daemon is neither stopped nor restarted. The staged files are swapped
    # in later by `cephadm switch-staged`, so the daemon's downtime is only
    # the stop/start of its container.
    STAGE = 'Stage'


# Files written by runscripts.write_service_scripts(). When a redeploy is
# staged they are written with STAGED_SUFFIX appended; switch_staged_unit_files()
# moves the live ones to PREV_SUFFIX and the staged ones into place.
UNIT_FILES = [
    'unit.run',
    'unit.stop',
    'unit.poststop',
    'unit.meta',
    'unit.image',
]
STAGED_SUFFIX = '.staged'
PREV_SUFFIX = '.prev'
