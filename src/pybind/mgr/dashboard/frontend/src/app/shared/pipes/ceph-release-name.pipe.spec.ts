import { CephReleaseNamePipe } from './ceph-release-name.pipe';

describe('CephReleaseNamePipe', () => {
  const pipe = new CephReleaseNamePipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms with correct version format', () => {
    const value =
      'ceph version 13.1.0-534-g23d3751b89 \
       (23d3751b897b31d2bda57aeaf01acb5ff3c4a9cd) nautilus (dev)';
    expect(pipe.transform(value)).toBe('nautilus');
  });

  it('transforms with wrong version format', () => {
    const value = 'foo';
    expect(pipe.transform(value)).toBe('foo');
  });
});
