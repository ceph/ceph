import { CephShortVersionPipe } from './ceph-short-version.pipe';

describe('CephShortVersionPipe', () => {
  const pipe = new CephShortVersionPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms with correct version format', () => {
    const value =
      'ceph version 13.1.0-534-g23d3751b89 \
       (23d3751b897b31d2bda57aeaf01acb5ff3c4a9cd) nautilus (dev)';
    expect(pipe.transform(value)).toBe('13.1.0-534-g23d3751b89');
  });

  it('transforms with wrong version format', () => {
    const value = 'foo';
    expect(pipe.transform(value)).toBe('foo');
  });
});
