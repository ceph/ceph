import { CephShortVersionPipe } from './ceph-short-version.pipe';

describe('CephShortVersionPipe', () => {
  it('create an instance', () => {
    const pipe = new CephShortVersionPipe();
    expect(pipe).toBeTruthy();
  });
});
