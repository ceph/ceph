import { PgStatusStylePipe } from './pg-status-style.pipe';

describe('PgStatusStylePipe', () => {
  const pipe = new PgStatusStylePipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms with pg status error', () => {
    const value = { 'incomplete+clean': 8 };
    expect(pipe.transform(value)).toEqual({ color: '#FF0000' });
  });

  it('transforms with pg status warning', () => {
    const value = { 'active': 8 };
    expect(pipe.transform(value)).toEqual({ color: '#FFC200' });
  });

  it('transforms with pg status other', () => {
    const value = { 'active+clean': 8 };
    expect(pipe.transform(value)).toEqual({ color: '#00BB00' });
  });
});
