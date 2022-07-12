export class Flag {
  code: 'noout' | 'noin' | 'nodown' | 'noup';
  name: string;
  description: string;
  value: boolean;
  clusterWide: boolean;
  indeterminate: boolean;
}
