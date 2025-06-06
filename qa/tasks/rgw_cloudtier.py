"""
rgw_cloudtier configuration routines
"""
import argparse
import logging

from teuthology import misc as teuthology
from teuthology.exceptions import ConfigError
from tasks.util.rgw import rgwadmin, wait_for_radosgw
from teuthology.task import Task

log = logging.getLogger(__name__)

class RGWCloudTier(Task):
    """
    Configure CloudTier storage class.

    To configure cloudtiering on any client::

        tasks:
        - ceph:
        - rgw:
        - rgw-cloudtier:
            client.0:
              cloud_storage_class:
              cloud_client:
              cloud_regular_storage_class:
              cloud_target_storage_class:
              cloud_retain_head_object:
              cloud_target_path:
              cloud_allow_read_through:
              cloud_read_through_restore_days:
              cloudtier_user:
                cloud_secret:
                cloud_access_key:

    """
    def __init__(self, ctx, config):
        super(RGWCloudTier, self).__init__(ctx, config)

    def setup(self):
        super(RGWCloudTier, self).setup()

        overrides = self.ctx.config.get('overrides', {})
        teuthology.deep_merge(self.config, overrides.get('rgw-cloudtier', {}))

        if not self.ctx.rgw:
            raise ConfigError('rgw-cloudtier must run after the rgw task')

        self.ctx.rgw_cloudtier = argparse.Namespace()
        self.ctx.rgw_cloudtier.config = self.config

        log.info('Configuring rgw cloudtier ...')
        clients = self.config.keys() # http://tracker.ceph.com/issues/20417
        for client in clients:
            client_config = self.config.get(client)
            if client_config is None:
                client_config = {}

            if client_config is not None:
                log.info('client %s - cloudtier config is -----------------%s ', client, client_config)
                # configuring cloudtier

                cloud_client = client_config.get('cloud_client')
                cloud_storage_class = client_config.get('cloud_storage_class')
                cloud_target_path = client_config.get('cloud_target_path')
                cloud_target_storage_class = client_config.get('cloud_target_storage_class')
                cloud_retain_head_object = client_config.get('cloud_retain_head_object')
                cloud_allow_read_through = client_config.get('cloud_allow_read_through')
                cloud_read_through_restore_days = client_config.get('cloud_read_through_restore_days')

                cloudtier_user = client_config.get('cloudtier_user')
                cloud_access_key = cloudtier_user.get('cloud_access_key')
                cloud_secret = cloudtier_user.get('cloud_secret')

                # XXX: the 'default' zone and zonegroup aren't created until we run RGWRados::init_complete().
                # issue a 'radosgw-admin user list' command to trigger this
                rgwadmin(self.ctx, client, cmd=['user', 'list'], check_status=True)

                endpoint = self.ctx.rgw.role_endpoints[cloud_client]

                # create cloudtier storage class
                tier_config_params = "endpoint=" + endpoint.url() + \
                           ",access_key=" + cloud_access_key + \
                            ",secret=" + cloud_secret + \
                            ",retain_head_object=" + cloud_retain_head_object

                if (cloud_target_path != None):
                    tier_config_params += ",target_path=" + cloud_target_path
                if (cloud_target_storage_class != None):
                    tier_config_params += ",target_storage_class=" + cloud_target_storage_class
                if (cloud_allow_read_through != None):
                    tier_config_params += ",allow_read_through=" + cloud_allow_read_through
                if (cloud_read_through_restore_days != None):
                    tier_config_params += ",read_through_restore_days=" + cloud_read_through_restore_days

                log.info('Configuring cloud-s3 tier storage class type = %s', cloud_storage_class)

                rgwadmin(self.ctx, client,
                      cmd=['zonegroup', 'placement', 'add',
                            '--rgw-zone', 'default',
                            '--placement-id', 'default-placement',
                            '--storage-class', cloud_storage_class,
                            '--tier-type', 'cloud-s3',
                            '--tier-config', tier_config_params],
                      check_status=True)

                ## create cloudtier user with the access keys given on the cloud client
                cloud_tier_user_id = "cloud-tier-user-" + cloud_client
                cloud_tier_user_name = "CLOUD TIER USER - " + cloud_client
                rgwadmin(self.ctx, cloud_client,
                     cmd=['user', 'create', '--uid', cloud_tier_user_id,
                        '--display-name', cloud_tier_user_name,
                        '--access-key', cloud_access_key,
                        '--secret', cloud_secret,
                        '--caps', 'user-policy=*'],
                        check_status=True)

                log.info('Finished Configuring rgw cloudtier ...')
                
                cluster_name, daemon_type, client_id = teuthology.split_role(client)
                client_with_id = daemon_type + '.' + client_id
                self.ctx.daemons.get_daemon('rgw', client_with_id, cluster_name).restart()
                log.info('restarted rgw daemon ...')

                (remote,) = self.ctx.cluster.only(client).remotes.keys()
                wait_for_radosgw(endpoint.url(), remote)
                

task = RGWCloudTier
