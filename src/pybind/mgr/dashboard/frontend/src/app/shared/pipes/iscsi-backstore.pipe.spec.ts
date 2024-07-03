import { IscsiBackstorePipe } from './iscsi-backstore.pipe';

describe('IscsiBackstorePipe', () => {
  const pipe = new IscsiBackstorePipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "user:rbd"', () => {
    expect(pipe.transform('user:rbd')).toBe('user:rbd (tcmu-runner)');
  });

  it('transforms "other"', () => {
    expect(pipe.transform('other')).toBe('other');
  });
});
