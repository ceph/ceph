import { PageHelper } from '../page-helper.po';

export class UrlsCollection extends PageHelper {
  pages = {
    // Cluster expansion
    welcome: { url: '#/expand-cluster?welcome=true', id: 'cd-create-cluster' },

    // Landing page
    dashboard: { url: '#/dashboard', id: 'cd-dashboard' },

    // Hosts
    hosts: { url: '#/hosts', id: 'cd-hosts' },
    'add hosts': { url: '#/hosts/(modal:add)', id: 'cd-host-form' },

    // Services
    services: { url: '#/services', id: 'cd-services' },
    'create services': { url: '#/services/(modal:create)', id: 'cd-service-form' },

    // Physical Disks
    'physical disks': { url: '#/inventory', id: 'cd-inventory' },

    // Monitors
    monitors: { url: '#/monitor', id: 'cd-monitor' },

    // OSDs
    osds: { url: '#/osd', id: 'cd-osd-list' },
    'create osds': { url: '#/osd/create', id: 'cd-osd-form' },

    // Configuration
    configuration: { url: '#/configuration', id: 'cd-configuration' },

    // Crush Map
    'crush map': { url: '#/crush-map', id: 'cd-crushmap' },

    // Mgr modules
    'mgr-modules': { url: '#/mgr-modules', id: 'cd-mgr-module-list' },

    // Logs
    logs: { url: '#/logs', id: 'cd-logs' },

    // RGW Daemons
    'rgw daemons': { url: '#/rgw/daemon', id: 'cd-rgw-daemon-list' },

    // CephFS
    cephfs: { url: '#/cephfs/fs', id: 'cd-cephfs-list' },
    'create cephfs': { url: '#/cephfs/fs/create', id: 'cd-cephfs-form' }
  };
}
