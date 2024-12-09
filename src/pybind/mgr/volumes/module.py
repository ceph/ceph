import errno
import logging
import traceback
import threading

from mgr_module import MgrModule, Option
import orchestrator

from .fs.volume import VolumeClient

log = logging.getLogger(__name__)

goodchars = '[A-Za-z0-9-_.]'


class VolumesInfoWrapper():
    def __init__(self, f, context):
        self.f = f
        self.context = context

    def __enter__(self):
        log.info("Starting {}".format(self.context))

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            log.error("Failed {}:\n{}".format(self.context, "".join(traceback.format_exception(exc_type, exc_value, tb))))
        else:
            log.info("Finishing {}".format(self.context))


def mgr_cmd_wrap(f):
    def wrap(self, inbuf, cmd):
        astr = []
        for k in cmd:
            astr.append("{}:{}".format(k, cmd[k]))
        context = "{}({}) < \"{}\"".format(f.__name__, ", ".join(astr), inbuf)
        with VolumesInfoWrapper(f, context):
            return f(self, inbuf, cmd)
    return wrap


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    COMMANDS = [
        {
            'cmd': 'fs volume ls',
            'desc': "List volumes",
            'perm': 'r'
        },
        {
            'cmd': 'fs volume create '
                   f'name=name,type=CephString,goodchars={goodchars} '
                   'name=placement,type=CephString,req=false ',
            'desc': "Create a CephFS volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs volume rm '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   'name=yes-i-really-mean-it,type=CephString,req=false ',
            'desc': "Delete a FS volume by passing --yes-i-really-mean-it flag",
            'perm': 'rw'
        },
        {
            'cmd': 'fs volume rename '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=new_vol_name,type=CephString,goodchars={goodchars} '
                   'name=yes_i_really_mean_it,type=CephBool,req=false ',
            'desc': "Rename a CephFS volume by passing --yes-i-really-mean-it flag",
            'perm': 'rw'
        },
        {
            'cmd': 'fs volume info '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   'name=human_readable,type=CephBool,req=false ',
            'desc': "Get the information of a CephFS volume",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolumegroup ls '
            f'name=vol_name,type=CephString,goodchars={goodchars} ',
            'desc': "List subvolumegroups",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolumegroup create '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,goodchars={goodchars} '
                   'name=size,type=CephInt,req=false '
                   'name=pool_layout,type=CephString,req=false '
                   'name=uid,type=CephInt,req=false '
                   'name=gid,type=CephInt,req=false '
                   'name=mode,type=CephString,req=false ',
            'desc': "Create a CephFS subvolume group in a volume, and optionally, "
                    "with a specific data pool layout, and a specific numeric mode",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup rm '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,goodchars={goodchars} '
                   'name=force,type=CephBool,req=false ',
            'desc': "Delete a CephFS subvolume group in a volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup info '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,goodchars={goodchars} ',
            'desc': "Get the metadata of a CephFS subvolume group in a volume, ",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolumegroup resize '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,goodchars={goodchars} '
                   'name=new_size,type=CephString,req=true '
                   'name=no_shrink,type=CephBool,req=false ',
            'desc': "Resize a CephFS subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup exist '
                   f'name=vol_name,type=CephString,goodchars={goodchars} ',
            'desc': "Check a volume for the existence of subvolumegroup",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume ls '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "List subvolumes",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume create '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=size,type=CephInt,req=false '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} '
                   'name=pool_layout,type=CephString,req=false '
                   'name=uid,type=CephInt,req=false '
                   'name=gid,type=CephInt,req=false '
                   'name=mode,type=CephString,req=false '
                   'name=namespace_isolated,type=CephBool,req=false '
                   'name=earmark,type=CephString,req=false ',
            'desc': "Create a CephFS subvolume in a volume, and optionally, "
                    "with a specific size (in bytes), a specific data pool layout, "
                    "a specific mode, in a specific subvolume group and in separate "
                    "RADOS namespace",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume rm '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} '
                   'name=force,type=CephBool,req=false '
                   'name=retain_snapshots,type=CephBool,req=false ',
            'desc': "Delete a CephFS subvolume in a volume, and optionally, "
                    "in a specific subvolume group, force deleting a cancelled or failed "
                    "clone, and retaining existing subvolume snapshots",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume authorize '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=auth_id,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} '
                   'name=access_level,type=CephString,req=false '
                   'name=tenant_id,type=CephString,req=false '
                   'name=allow_existing_id,type=CephBool,req=false ',
            'desc': "Allow a cephx auth ID access to a subvolume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume deauthorize '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=auth_id,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Deny a cephx auth ID access to a subvolume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume authorized_list '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "List auth IDs that have access to a subvolume",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume evict '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=auth_id,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Evict clients based on auth IDs and subvolume mounted",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup getpath '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,goodchars={goodchars} ',
            'desc': "Get the mountpath of a CephFS subvolume group in a volume",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume getpath '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Get the mountpath of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume info '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Get the information of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume exist '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Check a volume for the existence of a subvolume, "
                    "optionally in a specified subvolume group",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume metadata set '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=key_name,type=CephString '
                   'name=value,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Set custom metadata (key-value) for a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume metadata get '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=key_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Get custom metadata associated with the key of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume metadata ls '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "List custom metadata (key-value pairs) of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume metadata rm '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=key_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} '
                   'name=force,type=CephBool,req=false ',
            'desc': "Remove custom metadata (key-value) associated with the key of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume earmark get '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Get earmark for a subvolume",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume earmark set '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} '
                   'name=earmark,type=CephString ',
            'desc': "Set earmark for a subvolume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume earmark rm '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Remove earmark from a subvolume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs quiesce '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   'name=members,type=CephString,n=N,req=false '
                   '-- '
                   'name=set_id,type=CephString,req=false '
                   'name=timeout,type=CephFloat,range=0,req=false '
                   'name=expiration,type=CephFloat,range=0,req=false '
                   'name=await_for,type=CephFloat,range=0,req=false '
                   'name=await,type=CephBool,req=false '
                   'name=if_version,type=CephInt,range=0,req=false '
                   'name=include,type=CephBool,req=false '
                   'name=exclude,type=CephBool,req=false '
                   'name=reset,type=CephBool,req=false '
                   'name=release,type=CephBool,req=false '
                   'name=query,type=CephBool,req=false '
                   'name=all,type=CephBool,req=false '
                   'name=cancel,type=CephBool,req=false '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} '
                   'name=leader,type=CephBool,req=false '
                   'name=with_leader,type=CephInt,range=0,req=false ',
            'desc': "Manage quiesce sets of subvolumes",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup pin'
                   f' name=vol_name,type=CephString,goodchars={goodchars}'
                   f' name=group_name,type=CephString,req=true,goodchars={goodchars}'
                   ' name=pin_type,type=CephChoices,strings=export|distributed|random'
                   ' name=pin_setting,type=CephString,req=true',
            'desc': "Set MDS pinning policy for subvolumegroup",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup snapshot ls '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,goodchars={goodchars} ',
            'desc': "List subvolumegroup snapshots",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolumegroup snapshot create '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString ',
            'desc': "Create a snapshot of a CephFS subvolume group in a volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup snapshot rm '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString '
                   'name=force,type=CephBool,req=false ',
                   'desc': "Delete a snapshot of a CephFS subvolume group in a volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot ls '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "List subvolume snapshots",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume snapshot create '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Create a snapshot of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot info '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Get the information of a CephFS subvolume snapshot "
                    "and optionally, in a specific subvolume group",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume snapshot metadata set '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString '
                   'name=key_name,type=CephString '
                   'name=value,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Set custom metadata (key-value) for a CephFS subvolume snapshot in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot metadata get '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString '
                   'name=key_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Get custom metadata associated with the key of a CephFS subvolume snapshot in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume snapshot metadata ls '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "List custom metadata (key-value pairs) of a CephFS subvolume snapshot in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume snapshot metadata rm '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString '
                   'name=key_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} '
                   'name=force,type=CephBool,req=false ',
            'desc': "Remove custom metadata (key-value) associated with the key of a CephFS subvolume snapshot in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot rm '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} '
                   'name=force,type=CephBool,req=false ',
            'desc': "Delete a snapshot of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume resize '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=new_size,type=CephString,req=true '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} '
                   'name=no_shrink,type=CephBool,req=false ',
            'desc': "Resize a CephFS subvolume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume pin'
                   f' name=vol_name,type=CephString,goodchars={goodchars}'
                   f' name=sub_name,type=CephString,goodchars={goodchars}'
                   ' name=pin_type,type=CephChoices,strings=export|distributed|random'
                   ' name=pin_setting,type=CephString,req=true'
                   f' name=group_name,type=CephString,req=false,goodchars={goodchars}',
            'desc': "Set MDS pinning policy for subvolume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot protect '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "(deprecated) Protect snapshot of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot unprotect '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "(deprecated) Unprotect a snapshot of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot clone '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   f'name=sub_name,type=CephString,goodchars={goodchars} '
                   'name=snap_name,type=CephString '
                   'name=target_sub_name,type=CephString '
                   'name=pool_layout,type=CephString,req=false '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} '
                   'name=target_group_name,type=CephString,req=false ',
            'desc': "Clone a snapshot to target subvolume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs clone status '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   'name=clone_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Get status on a cloned subvolume.",
            'perm': 'r'
        },
        {
            'cmd': 'fs clone cancel '
                   f'name=vol_name,type=CephString,goodchars={goodchars} '
                   'name=clone_name,type=CephString '
                   f'name=group_name,type=CephString,req=false,goodchars={goodchars} ',
            'desc': "Cancel an pending or ongoing clone operation.",
            'perm': 'r'
        },
        # volume ls [recursive]
        # subvolume ls <volume>
        # volume authorize/deauthorize
        # subvolume authorize/deauthorize

        # volume describe (free space, etc)
        # volume auth list (vc.get_authorized_ids)

        # snapshots?

        # FIXME: we're doing CephFSVolumeClient.recover on every
        # path where we instantiate and connect a client.  Perhaps
        # keep clients alive longer, or just pass a "don't recover"
        # flag in if it's the >1st time we connected a particular
        # volume in the lifetime of this module instance.
    ]

    MODULE_OPTIONS = [
        Option(
            'max_concurrent_clones',
            type='int',
            default=4,
            desc='Number of asynchronous cloner threads'),
        Option(
            'snapshot_clone_delay',
            type='int',
            default=0,
            desc='Delay clone begin operation by snapshot_clone_delay seconds'),
        Option(
            'periodic_async_work',
            type='bool',
            default=False,
            desc='Periodically check for async work'),
        Option(
            'snapshot_clone_no_wait',
            type='bool',
            default=True,
            desc='Reject subvolume clone request when cloner threads are busy')
    ]

    def __init__(self, *args, **kwargs):
        self.inited = False
        # for mypy
        self.max_concurrent_clones = None
        self.snapshot_clone_delay = None
        self.periodic_async_work = False
        self.snapshot_clone_no_wait = None
        self.lock = threading.Lock()
        super(Module, self).__init__(*args, **kwargs)
        # Initialize config option members
        self.config_notify()
        with self.lock:
            self.vc = VolumeClient(self)
            self.inited = True

    def shutdown(self):
        self.vc.shutdown()

    def config_notify(self):
        """
        This method is called whenever one of our config options is changed.
        """
        with self.lock:
            for opt in self.MODULE_OPTIONS:
                setattr(self,
                        opt['name'],  # type: ignore
                        self.get_module_option(opt['name']))  # type: ignore
                self.log.debug(' mgr option %s = %s',
                               opt['name'], getattr(self, opt['name']))  # type: ignore
                if self.inited:
                    if opt['name'] == "max_concurrent_clones":
                        self.vc.cloner.reconfigure_max_concurrent_clones(self.max_concurrent_clones)
                    elif opt['name'] == "snapshot_clone_delay":
                        self.vc.cloner.reconfigure_snapshot_clone_delay(self.snapshot_clone_delay)
                    elif opt['name'] == "periodic_async_work":
                        if self.periodic_async_work:
                            self.vc.cloner.set_wakeup_timeout()
                            self.vc.purge_queue.set_wakeup_timeout()
                        else:
                            self.vc.cloner.unset_wakeup_timeout()
                            self.vc.purge_queue.unset_wakeup_timeout()
                    elif opt['name'] == "snapshot_clone_no_wait":
                        self.vc.cloner.reconfigure_reject_clones(self.snapshot_clone_no_wait)

    def _is_name_valid(self, name):
        if not name:  # for None ... e.g. we're okay when group_name is None
            return True
        sn = set(name)
        # leading dot creates hidden dirs for groups and subvolumes
        # that doesn't seem like a sensible thing to have
        if name.startswith('.') or sn == set('.') or sn == set('/') or sn == set('./'):
            return False
        return True

    def _are_names_valid(self, vol_name, group_name=None, sub_name=None):
        if not self._is_name_valid(vol_name):
            return -errno.EINVAL, "", "choose a better volume name"
        if not self._is_name_valid(group_name):
            return -errno.EINVAL, "", "choose a better subvolume group name"
        if not self._is_name_valid(sub_name):
            return -errno.EINVAL, "", "choose a better subvolume name"
        return 0, "", "Success"
            
    def handle_command(self, inbuf, cmd):
        handler_name = "_cmd_" + cmd['prefix'].replace(" ", "_")
        try:
            handler = getattr(self, handler_name)
        except AttributeError:
            return -errno.EINVAL, "", "Unknown command"

        return handler(inbuf, cmd)

    @mgr_cmd_wrap
    def _cmd_fs_volume_create(self, inbuf, cmd):
        vol_id = cmd['name']
        placement = cmd.get('placement', '')
        errno, out, err = self._are_names_valid(cmd['name'])
        if errno < 0:
            return errno, out, err
        return self.vc.create_fs_volume(vol_id, placement)

    @mgr_cmd_wrap
    def _cmd_fs_volume_rm(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        confirm = cmd.get('yes-i-really-mean-it', None)
        errno, out, err = self._are_names_valid(cmd['vol_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.delete_fs_volume(vol_name, confirm)

    @mgr_cmd_wrap
    def _cmd_fs_volume_ls(self, inbuf, cmd):
        return self.vc.list_fs_volumes()

    @mgr_cmd_wrap
    def _cmd_fs_volume_rename(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'])
        if errno < 0:
            return errno, out, "choose a name amongst the old volumes"
        errno, out, err = self._are_names_valid(cmd['new_vol_name'])
        if errno < 0:
            return errno, out, "choose a better name for the new volume"
        return self.vc.rename_fs_volume(cmd['vol_name'],
                                        cmd['new_vol_name'],
                                        cmd.get('yes_i_really_mean_it', False))

    @mgr_cmd_wrap
    def _cmd_fs_volume_info(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.volume_info(vol_name=cmd['vol_name'],
                                   human_readable=cmd.get('human_readable', False))

    @mgr_cmd_wrap
    def _cmd_fs_subvolumegroup_create(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd['group_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.create_subvolume_group(
            vol_name=cmd['vol_name'], group_name=cmd['group_name'], size=cmd.get('size', None),
            pool_layout=cmd.get('pool_layout', None), mode=cmd.get('mode', '755'),
            uid=cmd.get('uid', None), gid=cmd.get('gid', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolumegroup_rm(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd['group_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.remove_subvolume_group(vol_name=cmd['vol_name'],
                                              group_name=cmd['group_name'],
                                              force=cmd.get('force', False))

    @mgr_cmd_wrap
    def _cmd_fs_subvolumegroup_info(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd['group_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.subvolumegroup_info(vol_name=cmd['vol_name'],
                                           group_name=cmd['group_name'])

    @mgr_cmd_wrap
    def _cmd_fs_subvolumegroup_resize(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd['group_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.resize_subvolume_group(vol_name=cmd['vol_name'],
                                              group_name=cmd['group_name'],
                                              new_size=cmd['new_size'],
                                              no_shrink=cmd.get('no_shrink', False))

    @mgr_cmd_wrap
    def _cmd_fs_subvolumegroup_ls(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.list_subvolume_groups(vol_name=cmd['vol_name'])

    @mgr_cmd_wrap
    def _cmd_fs_subvolumegroup_exist(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.subvolume_group_exists(vol_name=cmd['vol_name'])

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_create(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.create_subvolume(vol_name=cmd['vol_name'],
                                        sub_name=cmd['sub_name'],
                                        group_name=cmd.get('group_name', None),
                                        size=cmd.get('size', None),
                                        pool_layout=cmd.get('pool_layout', None),
                                        uid=cmd.get('uid', None),
                                        gid=cmd.get('gid', None),
                                        mode=cmd.get('mode', '755'),
                                        namespace_isolated=cmd.get('namespace_isolated', False),
                                        earmark=cmd.get('earmark', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_rm(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.remove_subvolume(vol_name=cmd['vol_name'],
                                        sub_name=cmd['sub_name'],
                                        group_name=cmd.get('group_name', None),
                                        force=cmd.get('force', False),
                                        retain_snapshots=cmd.get('retain_snapshots', False))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_authorize(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), secret key(str), error message (str)
        """
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.authorize_subvolume(vol_name=cmd['vol_name'],
                                           sub_name=cmd['sub_name'],
                                           auth_id=cmd['auth_id'],
                                           group_name=cmd.get('group_name', None),
                                           access_level=cmd.get('access_level', 'rw'),
                                           tenant_id=cmd.get('tenant_id', None),
                                           allow_existing_id=cmd.get('allow_existing_id', False))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_deauthorize(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.deauthorize_subvolume(vol_name=cmd['vol_name'],
                                             sub_name=cmd['sub_name'],
                                             auth_id=cmd['auth_id'],
                                             group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_authorized_list(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), list of authids(json), error message (str)
        """
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.authorized_list(vol_name=cmd['vol_name'],
                                       sub_name=cmd['sub_name'],
                                       group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_evict(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empyt string(str), error message (str)
        """
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.evict(vol_name=cmd['vol_name'],
                             sub_name=cmd['sub_name'],
                             auth_id=cmd['auth_id'],
                             group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_ls(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None))
        if errno < 0:
            return errno, out, err
        return self.vc.list_subvolumes(vol_name=cmd['vol_name'],
                                       group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolumegroup_getpath(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None))
        if errno < 0:
            return errno, out, err
        return self.vc.getpath_subvolume_group(
            vol_name=cmd['vol_name'], group_name=cmd['group_name'])

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_getpath(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.subvolume_getpath(vol_name=cmd['vol_name'],
                                         sub_name=cmd['sub_name'],
                                         group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_info(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.subvolume_info(vol_name=cmd['vol_name'],
                                      sub_name=cmd['sub_name'],
                                      group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_exist(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None))
        if errno < 0:
            return errno, out, err
        return self.vc.subvolume_exists(vol_name=cmd['vol_name'],
                                        group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_metadata_set(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.set_user_metadata(vol_name=cmd['vol_name'],
                                      sub_name=cmd['sub_name'],
                                      key_name=cmd['key_name'],
                                      value=cmd['value'],
                                      group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_metadata_get(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.get_user_metadata(vol_name=cmd['vol_name'],
                                      sub_name=cmd['sub_name'],
                                      key_name=cmd['key_name'],
                                      group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_metadata_ls(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.list_user_metadata(vol_name=cmd['vol_name'],
                                      sub_name=cmd['sub_name'],
                                      group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_metadata_rm(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.remove_user_metadata(vol_name=cmd['vol_name'],
                                      sub_name=cmd['sub_name'],
                                      key_name=cmd['key_name'],
                                      group_name=cmd.get('group_name', None),
                                      force=cmd.get('force', False))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_earmark_get(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.get_earmark(vol_name=cmd['vol_name'],
                                   sub_name=cmd['sub_name'],
                                   group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_earmark_set(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.set_earmark(vol_name=cmd['vol_name'],
                                      sub_name=cmd['sub_name'],
                                      group_name=cmd.get('group_name', None),
                                      earmark=cmd['earmark'])

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_earmark_rm(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.clear_earmark(vol_name=cmd['vol_name'],
                                      sub_name=cmd['sub_name'],
                                      group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_quiesce(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None))
        if errno < 0:
            return errno, out, err
        return self.vc.quiesce(cmd)

    @mgr_cmd_wrap
    def _cmd_fs_subvolumegroup_pin(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd['group_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.pin_subvolume_group(vol_name=cmd['vol_name'],
                                           group_name=cmd['group_name'], pin_type=cmd['pin_type'],
                                           pin_setting=cmd['pin_setting'])

    @mgr_cmd_wrap
    def _cmd_fs_subvolumegroup_snapshot_create(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd['group_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.create_subvolume_group_snapshot(vol_name=cmd['vol_name'],
                                                       group_name=cmd['group_name'],
                                                       snap_name=cmd['snap_name'])

    @mgr_cmd_wrap
    def _cmd_fs_subvolumegroup_snapshot_rm(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd['group_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.remove_subvolume_group_snapshot(vol_name=cmd['vol_name'],
                                                       group_name=cmd['group_name'],
                                                       snap_name=cmd['snap_name'],
                                                       force=cmd.get('force', False))

    @mgr_cmd_wrap
    def _cmd_fs_subvolumegroup_snapshot_ls(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd['group_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.list_subvolume_group_snapshots(vol_name=cmd['vol_name'],
                                                      group_name=cmd['group_name'])

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_snapshot_create(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.create_subvolume_snapshot(vol_name=cmd['vol_name'],
                                                 sub_name=cmd['sub_name'],
                                                 snap_name=cmd['snap_name'],
                                                 group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_snapshot_rm(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.remove_subvolume_snapshot(vol_name=cmd['vol_name'],
                                                 sub_name=cmd['sub_name'],
                                                 snap_name=cmd['snap_name'],
                                                 group_name=cmd.get('group_name', None),
                                                 force=cmd.get('force', False))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_snapshot_info(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.subvolume_snapshot_info(vol_name=cmd['vol_name'],
                                               sub_name=cmd['sub_name'],
                                               snap_name=cmd['snap_name'],
                                               group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_snapshot_metadata_set(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.set_subvolume_snapshot_metadata(vol_name=cmd['vol_name'],
                                               sub_name=cmd['sub_name'],
                                               snap_name=cmd['snap_name'],
                                               key_name=cmd['key_name'],
                                               value=cmd['value'],
                                               group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_snapshot_metadata_get(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.get_subvolume_snapshot_metadata(vol_name=cmd['vol_name'],
                                               sub_name=cmd['sub_name'],
                                               snap_name=cmd['snap_name'],
                                               key_name=cmd['key_name'],
                                               group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_snapshot_metadata_ls(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.list_subvolume_snapshot_metadata(vol_name=cmd['vol_name'],
                                               sub_name=cmd['sub_name'],
                                               snap_name=cmd['snap_name'],
                                               group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_snapshot_metadata_rm(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.remove_subvolume_snapshot_metadata(vol_name=cmd['vol_name'],
                                               sub_name=cmd['sub_name'],
                                               snap_name=cmd['snap_name'],
                                               key_name=cmd['key_name'],
                                               group_name=cmd.get('group_name', None),
                                               force=cmd.get('force', False))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_snapshot_ls(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.list_subvolume_snapshots(vol_name=cmd['vol_name'],
                                                sub_name=cmd['sub_name'],
                                                group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_resize(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.resize_subvolume(vol_name=cmd['vol_name'], sub_name=cmd['sub_name'],
                                        new_size=cmd['new_size'], group_name=cmd.get('group_name', None),
                                        no_shrink=cmd.get('no_shrink', False))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_pin(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.subvolume_pin(vol_name=cmd['vol_name'],
                                     sub_name=cmd['sub_name'], pin_type=cmd['pin_type'],
                                     pin_setting=cmd['pin_setting'],
                                     group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_snapshot_protect(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.protect_subvolume_snapshot(vol_name=cmd['vol_name'], sub_name=cmd['sub_name'],
                                                  snap_name=cmd['snap_name'], group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_snapshot_unprotect(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.unprotect_subvolume_snapshot(vol_name=cmd['vol_name'], sub_name=cmd['sub_name'],
                                                    snap_name=cmd['snap_name'], group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_subvolume_snapshot_clone(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None), cmd['sub_name'])
        if errno < 0:
            return errno, out, err
        return self.vc.clone_subvolume_snapshot(
            vol_name=cmd['vol_name'], sub_name=cmd['sub_name'], snap_name=cmd['snap_name'],
            group_name=cmd.get('group_name', None), pool_layout=cmd.get('pool_layout', None),
            target_sub_name=cmd['target_sub_name'], target_group_name=cmd.get('target_group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_clone_status(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None))
        if errno < 0:
            return errno, out, err
        return self.vc.clone_status(
            vol_name=cmd['vol_name'], clone_name=cmd['clone_name'], group_name=cmd.get('group_name', None))

    @mgr_cmd_wrap
    def _cmd_fs_clone_cancel(self, inbuf, cmd):
        errno, out, err = self._are_names_valid(cmd['vol_name'], cmd.get('group_name', None))
        if errno < 0:
            return errno, out, err
        return self.vc.clone_cancel(
            vol_name=cmd['vol_name'], clone_name=cmd['clone_name'], group_name=cmd.get('group_name', None))

    # remote method
    def subvolume_getpath(self, vol_name, subvol, group_name):
        return self.vc.subvolume_getpath(vol_name=vol_name,
                                         sub_name=subvol,
                                         group_name=group_name)

    # remote method
    def subvolume_ls(self, vol_name, group_name):
        return self.vc.list_subvolumes(vol_name=vol_name, group_name=group_name)

    # remote method
    def subvolume_info(self, vol_name, subvol, group_name):
        return self.vc.subvolume_info(vol_name=vol_name,
                                      sub_name=subvol,
                                      group_name=group_name)
