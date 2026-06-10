export enum Icons {
  /* Icons for Symbol */
  add = 'add', // Create, Add
  addCircle = 'add--filled', // Plus with Circle
  minusCircle = 'subtract--alt', // Minus with Circle
  edit = 'edit', // Edit, Edit Mode, Rename
  destroy = 'close', // Destroy, Remove, Delete
  exchange = 'arrows--horizontal', // Edit-Peer
  copy = 'copy', // Copy
  clipboard = 'copy--file', // Clipboard
  flatten = 'unlink', // Flatten, Link broken, Mark Lost
  trash = 'trash-can', // Move to trash
  lock = 'locked', // Protect
  unlock = 'unlocked', // Unprotect
  clone = 'copy', // clone
  undo = 'undo', // Rollback, Restore
  search = 'search', // Search
  start = 'play', // Enable
  stop = 'stop--filled--alt', // Disable
  analyse = 'stethoscope', // Scrub
  deepCheck = 'settings', // Deep Scrub, Setting, Configuration
  cogs = 'settings--adjust', // Multiple Settings, Configurations
  reweight = 'scales', // Reweight
  up = 'arrow--up', // Up
  left = 'arrow--left', // Mark out
  right = 'arrow--right', // Mark in
  down = 'arrow--down', // Mark Down
  erase = 'erase', // Purge  color: bd.$white;
  expand = 'maximize', // Expand cluster
  user = 'user', // User, Initiators
  users = 'user--multiple', // Users, Groups
  share = 'share', // share
  key = 'password', // S3 Keys, Swift Keys, Authentication
  warning = 'warning--alt--filled', // Notification warning
  info = 'information', // Notification information
  infoCircle = 'information--filled', // Info on landing page
  questionCircle = 'help',
  danger = 'warning--filled',
  success = 'checkmark--filled',
  check = 'checkmark', // Notification check
  show = 'view', // Show
  paragraph = 'text-align-left', // Silence Matcher - Attribute name
  terminal = 'terminal', // Silence Matcher - Value
  magic = 'magic-wand', // Silence Matcher - Regex checkbox
  hourglass = 'hourglass', // Task
  filledHourglass = 'hourglass', // Task
  table = 'data-table', // Table,
  spinner = 'loading',
  refresh = 'renew', // Refresh
  bullseye = 'target', // Target
  disk = 'hard--drive', // Hard disk, disks
  server = 'server--rack', // Server, Portal
  filter = 'filter', // Filter
  lineChart = 'analytics', // Line chart
  signOut = 'logout', // Sign Out
  circle = 'dot-mark', // Circle
  bell = 'notification', // Notification
  mute = 'notification--off', // Mute or silence
  leftArrow = 'caret--left', // Left facing angle
  rightArrow = 'caret--right', // Right facing angle
  downArrow = 'caret--down',
  angleDoubleLeft = 'chevron--left', // Double left angle for pagination
  angleDoubleRight = 'chevron--right', // Double right angle for pagination
  square = 'checkbox', // Empty checkbox/square outline
  flag = 'flag', // OSD configuration
  clearFilters = 'close--filled', // Clear filters, solid x
  download = 'download', // Download
  upload = 'upload', // Upload
  code = 'code', // JSON file
  document = 'document', // Text file
  wrench = 'tools', // Configuration Error
  enter = 'login', // Enter
  exit = 'logout', // Exit
  restart = 'renew', // Restart
  deploy = 'cube', // Deploy, Redeploy
  cubes = 'cube', // Object storage
  sitemap = 'network--3', // Cluster, network, connections
  database = 'datastore', // Database, Block storage
  bars = 'menu', // Stack, bars
  navicon = 'menu', // Navigation
  areaChart = 'chart--area', // Area Chart, dashboard
  eye = 'view', // Observability
  calendar = 'calendar',
  externalUrl = 'launch', // links to external page
  nfsExport = 'server--rack', // NFS export
  launch = 'launch',
  dataTable = 'data-table',
  idea = 'idea',
  userAccessLocked = 'user--access-locked', // User access locked
  chevronDown = 'chevron--down',
  connect = 'connect',
  checkmarkOutline = 'checkmark--outline',
  circleDash = 'circle-dash',
  datastore = 'datastore',
  ibmCloudBareMetalServer = 'ibm-cloud--bare-metal-server',
  ibmCloudDedicatedHost = 'ibm-cloud--dedicated-host',
  clusterIcon = 'web-services--cluster',
  /* Icon sizes */
  size16 = '16',
  size20 = '20',
  size24 = '24',
  size32 = '32',
  /* Icons - Use IconSize enum for sizing instead of these deprecated values */
  notification = 'notification',
  error = 'error--filled',
  notificationOff = 'notification--off',
  notificationNew = 'notification--new',
  emptySearch = 'search',
  dataViewAlt = 'data--view--alt',
  dataCenter = 'data--center',
  upgrade = 'upgrade',
  warningAltFilled = 'warning--alt--filled',
  help = 'help',
  incidentReporter = 'incident-reporter',
  ibmStreamSets = 'ibm--streamsets',
  dataEnrichment = 'data-enrichment',
  network1 = 'network--1',
  chip = 'chip',
  plug = 'plug',
  vmdkDisk = 'vmdk-disk',
  checkMarkOutline = 'checkmark--outline',
  warningAlt = 'warning--alt',
  arrowUpRight = 'arrow--up-right',
  inProgress = 'in-progress',
  arrowDown = 'arrow--down',
  locked = 'locked', // Access denied, locked state
  cloudMonitoring = 'cloud--monitoring'
}

export enum IconSize {
  size16 = '16',
  size20 = '20',
  size24 = '24',
  size32 = '32'
}

export const ICON_TYPE = {
  check: 'check',
  copy: 'copy',
  danger: 'danger',
  deploy: 'deploy',
  edit: 'edit',
  error: 'error--filled',
  infoCircle: 'info-circle',
  notification: 'notification',
  notificationOff: 'notification--off',
  notificationNew: 'notification--new',
  success: 'success',
  warning: 'warning',
  add: 'add',
  emptySearch: 'emptySearch',
  dataViewAlt: 'data--view--alt',
  dataCenter: 'data--center',
  upgrade: 'upgrade',
  warningAltFilled: 'warning--alt--filled',
  help: 'help',
  incidentReporter: 'incident-reporter',
  ibmStreamSets: 'ibm--streamsets',
  dataEnrichment: 'data-enrichment',
  network1: 'network--1',
  chip: 'chip',
  plug: 'plug',
  vmdkDisk: 'vmdk-disk',
  warningAlt: 'warning--alt',
  checkMarkOutline: 'checkmark--outline',
  arrowUpRight: ' arrow--up-right',
  inProgress: 'in-progress',
  arrowDown: 'arrow--down',
  destroy: 'close',
  launch: 'launch',
  cubes: 'cube',
  angleDoubleLeft: 'chevron--left',
  angleDoubleRight: 'chevron--right',
  leftArrow: 'caret--left',
  rightArrow: 'caret--right',
  locked: 'locked',
  cloudMonitoring: 'cloud--monitoring',
  trash: 'trash-can'
} as const;

export const EMPTY_STATE_IMAGE = {
  default: 'assets/empty-state.png',
  search: 'assets/empty-state-search.png',
  locked: 'assets/locked.png'
} as const;
