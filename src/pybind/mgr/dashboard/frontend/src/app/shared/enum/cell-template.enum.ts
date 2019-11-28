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
  //   customTemplateConfig: {
  //     class?: string; // Additional class name.
  //     prefix?: any;   // Prefix of the value to be displayed.
  //                     // 'map' and 'prefix' exclude each other.
  //     map?: {
  //       [key: any]: { value: any, class?: string }
  //     }
  //   }
  // }
  badge = 'badge'
}
