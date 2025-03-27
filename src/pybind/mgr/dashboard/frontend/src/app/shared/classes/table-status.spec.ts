import { TableStatus } from './table-status';

describe('TableStatus', () => {
  it('should create an instance', () => {
    const ts = new TableStatus();
    expect(ts).toBeTruthy();
    expect(ts).toEqual({ msg: '', type: 'ghost' });
  });

  it('should create with parameters', () => {
    const ts = new TableStatus('danger', 'foo');
    expect(ts).toBeTruthy();
    expect(ts).toEqual({ msg: 'foo', type: 'danger' });
  });
});
