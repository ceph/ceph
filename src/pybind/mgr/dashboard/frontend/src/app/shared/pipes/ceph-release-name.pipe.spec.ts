import { CephReleaseNamePipe } from './ceph-release-name.pipe';
import { VERSION_PREFIX } from '~/app/shared/constants/app.constants';

describe('CephReleaseNamePipe', () => {
  const pipe = new CephReleaseNamePipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('recognizes a stable release', () => {
    const value = `${VERSION_PREFIX} 13.2.1 \
       (5533ecdc0fda920179d7ad84e0aa65a127b20d77) mimic (stable)`;
    expect(pipe.transform(value)).toBe('mimic');
  });

  it('recognizes a development release as the main branch', () => {
    const value = `${VERSION_PREFIX} 13.1.0-534-g23d3751b89 \
       (23d3751b897b31d2bda57aeaf01acb5ff3c4a9cd) nautilus (dev)`;
    expect(pipe.transform(value)).toBe('main');
  });

  it('transforms with wrong version format', () => {
    const value = 'foo';
    expect(pipe.transform(value)).toBe('foo');
  });
});
