export enum Icons {
  /* Icons for Symbol */
  copy = 'copy', // Copy
  clipboard = 'copy--file', // Clipboard
  search = 'search', // Search
  analyse = 'fa fa-stethoscope', // Scrub
  cogs = 'fa fa-cogs', // Multiple Settings, Configurations
  reweight = 'scales', // Reweight
  up = 'arrow--up', // Up
  erase = 'fa fa-eraser', // Purge  color: bd.$white;
  user = 'user', // User, Initiators
  users = 'user--multiple', // Users, Groups
  share = 'share', // share
  key = 'password', // S3 Keys, Swift Keys, Authentication
  warning = 'warning--alt--filled', // Notification warning
  infoCircle = 'information--filled', // Info on landing page
  questionCircle = 'help',
  success = 'checkmark--filled',
  check = 'checkmark', // Notification check
  paragraph = 'fa fa-paragraph', // Silence Matcher - Attribute name
  terminal = 'fa fa-terminal', // Silence Matcher - Value
  magic = 'fa fa-magic', // Silence Matcher - Regex checkbox
  hourglass = 'fa fa-hourglass-o', // Task
  filledHourglass = 'fa fa-hourglass', // Task
  table = 'fa fa-table', // Table,
  spinner = 'fa fa-spinner',
  refresh = 'renew', // Refresh
  bullseye = 'fa fa-bullseye', // Target
  disk = 'fa fa-hdd-o', // Hard disk, disks
  server = 'fa fa-server', // Server, Portal
  filter = 'filter', // Filter
  lineChart = 'analytics', // Line chart
  circle = 'dot-mark', // Circle
  bell = 'notification', // Notification
  mute = 'notification--off', // Mute or silence
  leftArrow = 'caret--left', // Left facing angle
  rightArrow = 'caret--right', // Right facing angle
  downArrow = 'caret--down',
  download = 'download', // Download
  code = 'code', // JSON file
  document = 'document', // Text file
  deploy = 'cube', // Deploy, Redeploy
  sitemap = 'fa fa-sitemap', // Cluster, network, connections
  database = 'fa fa-database', // Database, Block storage
  navicon = 'fa fa-navicon', // Navigation
  areaChart = 'fa fa-area-chart', // Area Chart, dashboard
  eye = 'fa fa-eye', // Observability
  calendar = 'fa fa-calendar',
  externalUrl = 'fa fa-external-link', // links to external page
  parentChild = 'parent-child',
  dataTable = 'data-table',
  idea = 'idea',
  userAccessLocked = 'user--access-locked', // User access locked
  connect = 'connect',
  /* Icons for special effect */
  size16 = '16',
  size20 = '20',
  size24 = '24',
  size32 = '32',
  large = 'fa fa-lg', // icon becomes 33% larger
  large2x = 'fa fa-2x', // icon becomes 50% larger
  large3x = 'fa fa-3x', // icon becomes 3 times larger
  stack = 'fa fa-stack', // To stack multiple icons
  stack1x = 'fa fa-stack-1x', // To stack regularly sized icon
  stack2x = 'fa fa-stack-2x', // To stack regularly sized icon
  pulse = 'fa fa-pulse', // To have spinner rotate with 8 steps
  spin = 'fa fa-spin', //  To get any icon to rotate
  inverse = 'fa fa-inverse', // To get an alternative icon color
  notification = 'notification',
  error = 'error--filled',
  notificationOff = 'notification--off',
  notificationNew = 'notification--new'
}

export enum IconSize {
  size16 = '16',
  size20 = '20',
  size24 = '24',
  size32 = '32'
}

export const ICON_TYPE = {
  add: 'add',
  bell: 'bell',
  check: 'check',
  circle: 'circle',
  closeFilled: 'close--filled',
  code: 'code',
  copy: 'copy',
  danger: 'danger',
  dataTable: 'dataTable',
  deepCheck: 'deepCheck',
  deploy: 'deploy',
  destroy: 'destroy',
  document: 'document',
  download: 'download',
  edit: 'edit',
  error: 'error--filled',
  filter: 'filter',
  idea: 'idea',
  infoCircle: 'info-circle',
  key: 'key',
  launch: 'launch',
  lineChart: 'lineChart',
  minusCircle: 'subtract--alt',
  mute: 'mute',
  notification: 'notification',
  notificationOff: 'notification--off',
  notificationNew: 'notification--new',
  parentChild: 'parentChild',
  refresh: 'refresh',
  reweight: 'reweight',
  search: 'search',
  show: 'show',
  success: 'success',
  trash: 'trash-can',
  up: 'up',
  userAccessLocked: 'userAccessLocked',
  warning: 'warning',
  undo: 'undo',
  erase: 'erase',
  arrowDown: 'arrow--down',
  dataClass: 'data-class',
  scales: 'scales',
  tools: 'tools',
  tag: 'tag',
  settings: 'settings',
  close: 'close',
  warningFilled: 'warning--filled',
  chevronDown: 'chevron--down',
  addCircle: 'add--filled',
  exchange: 'arrows--horizontal',
  unlink: 'unlink',
  lock: 'locked',
  unlock: 'unlocked',
  clone: 'replicate',
  logout: 'logout',
  dataCards: 'show-data--cards',
  bareMetalServer: 'bare-metal-server',
  maximize: 'maximize',
  login: 'login',
  view: 'view',
  viewOff: 'view--off',
  playFilled: 'play--filled--alt',
  stopFilled: 'stop--filled--alt',
  flag: 'flag',
  arrowLeft: 'arrow--left',
  arrowRight: 'arrow--right',
  restart: 'restart',
  upload: 'upload',
  cube: 'cube',
  info: 'information',
} as const;
