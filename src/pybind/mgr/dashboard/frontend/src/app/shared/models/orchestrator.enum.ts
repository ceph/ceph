export enum OrchestratorFeature {
  HOST_LIST = 'get_hosts',
  HOST_ADD = 'add_host',
  HOST_REMOVE = 'remove_host',
  HOST_LABEL_ADD = 'add_host_label',
  HOST_LABEL_REMOVE = 'remove_host_label',
  HOST_MAINTENANCE_ENTER = 'enter_host_maintenance',
  HOST_MAINTENANCE_EXIT = 'exit_host_maintenance',
  HOST_FACTS = 'get_facts',
  HOST_DRAIN = 'drain_host',

  SERVICE_LIST = 'describe_service',
  SERVICE_CREATE = 'apply',
  SERVICE_EDIT = 'apply',
  SERVICE_DELETE = 'remove_service',
  SERVICE_RELOAD = 'service_action',
  DAEMON_LIST = 'list_daemons',

  OSD_GET_REMOVE_STATUS = 'remove_osds_status',
  OSD_CREATE = 'apply_drivegroups',
  OSD_DELETE = 'remove_osds',

  DEVICE_LIST = 'get_inventory',
  DEVICE_BLINK_LIGHT = 'blink_device_light'
}
