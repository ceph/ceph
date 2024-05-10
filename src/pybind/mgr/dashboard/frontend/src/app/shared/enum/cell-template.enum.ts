export enum CellTemplate {
  bold = 'bold',
  sparkline = 'sparkline',
  perSecond = 'perSecond',
  checkIcon = 'checkIcon',
  routerLink = 'routerLink',
  // Display the cell with an executing state. The state can be set to the `cdExecuting`
  // attribute of table rows.
  // It supports an optional custom configuration:
  // {
  //   ...
  //   cellTransformation: CellTemplate.executing,
  //   customTemplateConfig: {
  //     valueClass?: string;       // Cell value classes.
  //     executingClass?: string;   // Executing state classes.
  // }
  executing = 'executing',
  classAdding = 'classAdding',
  // Display the cell value as a badge. The template
  // supports an optional custom configuration:
  // {
  //   ...
  //   cellTransformation: CellTemplate.badge,
  //   customTemplateConfig: {
  //     class?: string; // Additional class name.
  //     prefix?: any;   // Prefix of the value to be displayed.
  //                     // 'map' and 'prefix' exclude each other.
  //     map?: {
  //       [key: any]: { value: any, class?: string }
  //     }
  //   }
  // }
  badge = 'badge',
  // Maps the value using the given dictionary.
  // {
  //   ...
  //   cellTransformation: CellTemplate.map,
  //   customTemplateConfig: {
  //     [key: any]: any
  //   }
  // }
  map = 'map',
  // Truncates string if it's longer than the given maximum
  // string length.
  // {
  //   ...
  //   cellTransformation: CellTemplate.truncate,
  //   customTemplateConfig: {
  //     length?: number;   // Defaults to 30.
  //     omission?: string; // Defaults to empty string.
  //   }
  // }
  truncate = 'truncate',
  /*
  This templace replaces a time, datetime or timestamp with a user-friendly "X {seconds,minutes,hours,days,...} ago",
  but the tooltip still displays the absolute timestamp
  */
  timeAgo = 'timeAgo',
  /*
  This template truncates a path to a shorter format and shows the whole path in a tooltip
  eg: /var/lib/ceph/osd/ceph-0 -> /var/.../ceph-0
  */
  path = 'path'
}
