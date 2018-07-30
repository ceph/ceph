import { PgStatusPipe } from './pg-status.pipe';

describe('PgStatusPipe', () => {
  const pipe = new PgStatusPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms with 1 status', () => {
    const value = { 'active+clean': 8 };
    expect(pipe.transform(value)).toBe('8 active+clean');
  });

  it('transforms with 2 status', () => {
    const value = { active: 8, incomplete: 8 };
    expect(pipe.transform(value)).toBe('8 active, 8 incomplete');
  });
});
