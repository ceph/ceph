export class TableStatus {
  constructor(
    public type: 'primary' | 'secondary' | 'danger' | 'ghost' = 'ghost',
    public msg = ''
  ) {}
}
