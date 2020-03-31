export enum CellTemplate {
  bold = 'bold',
  sparkline = 'sparkline',
  perSecond = 'perSecond',
  checkIcon = 'checkIcon',
  routerLink = 'routerLink',
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
  truncate = 'truncate'
}
