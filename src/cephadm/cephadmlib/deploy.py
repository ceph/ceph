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
