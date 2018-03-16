import { PgStatusPipe } from './pg-status.pipe';

describe('PgStatusPipe', () => {
  it('create an instance', () => {
    const pipe = new PgStatusPipe();
    expect(pipe).toBeTruthy();
  });
});
