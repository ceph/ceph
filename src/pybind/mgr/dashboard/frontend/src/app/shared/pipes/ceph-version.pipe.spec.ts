import { VERSION_PREFIX } from '~/app/shared/constants/app.constants';
import { CephVersionPipe } from './ceph-version.pipe';

describe('CephVersionPipe', () => {
  const pipe = new CephVersionPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('extracts version correctly', () => {
    const value = `${VERSION_PREFIX} 20.3.0-5182-g70be2125 (70be21257b5dac58119850e36211f267cc8b541a) tentacle (dev - RelWithDebInfo)`;
    expect(pipe.transform(value)).toBe('20.3.0-5182-g70be2125 tentacle (dev - RelWithDebInfo)');
  });
});
