export class TableStatus {
  constructor(public type: 'info' | 'warning' | 'danger' | 'light' = 'light', public msg = '') {}
}
