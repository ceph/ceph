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
  clone = 'fa fa-clone', // clone
  undo = 'fa fa-undo', // Rollback, Restore
  search = 'search', // Search
  start = 'fa fa-play', // Enable
  stop = 'fa fa-stop', // Disable
  analyse = 'fa fa-stethoscope', // Scrub
  deepCheck = 'settings', // Deep Scrub, Setting, Configuration
  cogs = 'fa fa-cogs', // Multiple Settings, Configurations
  reweight = 'scales', // Reweight
  up = 'arrow--up', // Up
  left = 'arrow--left', // Mark out
  right = 'arrow--right', // Mark in
  down = 'arrow--down', // Mark Down
  erase = 'fa fa-eraser', // Purge  color: bd.$white;
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
  signOut = 'fa fa-sign-out', // Sign Out
  circle = 'dot-mark', // Circle
  bell = 'notification', // Notification
  mute = 'notification--off', // Mute or silence
  leftArrow = 'caret--left', // Left facing angle
  rightArrow = 'caret--right', // Right facing angle
  downArrow = 'caret--down',
  flag = 'fa fa-flag', // OSD configuration
  clearFilters = 'close--filled', // Clear filters, solid x
  download = 'download', // Download
  upload = 'fa fa-upload', // Upload
  code = 'code', // JSON file
  document = 'document', // Text file
  wrench = 'tools', // Configuration Error
  enter = 'fa fa-sign-in', // Enter
  exit = 'fa fa-sign-out', // Exit
  restart = 'fa fa-history', // Restart
  deploy = 'cube', // Deploy, Redeploy
  cubes = 'fa fa-cubes', // Object storage
  sitemap = 'fa fa-sitemap', // Cluster, network, connections
  database = 'fa fa-database', // Database, Block storage
  bars = 'fa fa-bars', // Stack, bars
  navicon = 'fa fa-navicon', // Navigation
  areaChart = 'fa fa-area-chart', // Area Chart, dashboard
  eye = 'fa fa-eye', // Observability
  calendar = 'fa fa-calendar',
  externalUrl = 'fa fa-external-link', // links to external page
  nfsExport = 'fa fa-server', // NFS export
  launch = 'launch',
  parentChild = 'parent-child',
  dataTable = 'data-table',
  idea = 'idea',
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
  inverse = 'fa fa-inverse' // To get an alternative icon color
}

export enum IconSize {
  size16 = '16',
  size20 = '20',
  size24 = '24',
  size32 = '32'
}

export const ICON_TYPE = {
  copy: 'copy',
  danger: 'danger',
  infoCircle: 'info-circle',
  success: 'success',
  warning: 'warning'
} as const;
