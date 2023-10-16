import { PageHelper } from '../page-helper.po';

export class UrlsCollection extends PageHelper {
  pages = {
    // Cluster expansion
    welcome: { url: '#/cluster/expand-cluster', id: 'cd-create-cluster' },

    // Landing page
    dashboard: { url: '#/dashboard', id: 'cd-dashboard' },

    // Hosts
    hosts: { url: '#/cluster/hosts', id: 'cd-hosts' },
    'add hosts': { url: '#/cluster/hosts/(modal:add)', id: 'cd-host-form' },

    // Services
    services: { url: '#/admin/services', id: 'cd-services' },
    'create services': { url: '#/admin/services/(modal:create)', id: 'cd-service-form' },

    // Physical Disks
    'physical disks': { url: '#/cluster/inventory', id: 'cd-inventory' },

    // Monitors
    monitors: { url: '#/cluster/monitor', id: 'cd-monitor' },

    // OSDs
    osds: { url: '#/cluster/osd', id: 'cd-osd-list' },
    'create osds': { url: '#/cluster/osd/create', id: 'cd-osd-form' },

    // Configuration
    configuration: { url: '#/admin/configuration', id: 'cd-configuration' },

    // Crush Map
    'crush map': { url: '#/cluster/crush-map', id: 'cd-crushmap' },

    // Mgr modules
    'mgr-modules': { url: '#/admin/mgr-modules', id: 'cd-mgr-module-list' },

    // Logs
    logs: { url: '#/observability/logs', id: 'cd-logs' },

    // RGW Daemons
    'rgw daemons': { url: '#/rgw/daemon', id: 'cd-rgw-daemon-list' },

    // CephFS
    cephfs: { url: '#/file/cephfs', id: 'cd-cephfs-list' },
    'create cephfs': { url: '#/file/cephfs/create', id: 'cd-cephfs-form' }
  };
}
