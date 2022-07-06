import { PlacementPipe } from './placement.pipe';

describe('PlacementPipe', () => {
  const pipe = new PlacementPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms to no spec', () => {
    expect(pipe.transform(undefined)).toBe('no spec');
  });

  it('transforms to unmanaged', () => {
    expect(pipe.transform({ unmanaged: true })).toBe('unmanaged');
  });

  it('transforms placement (1)', () => {
    expect(
      pipe.transform({
        placement: {
          hosts: ['mon0']
        }
      })
    ).toBe('mon0');
  });

  it('transforms placement (2)', () => {
    expect(
      pipe.transform({
        placement: {
          hosts: ['mon0', 'mgr0']
        }
      })
    ).toBe('mon0;mgr0');
  });

  it('transforms placement (3)', () => {
    expect(
      pipe.transform({
        placement: {
          count: 1
        }
      })
    ).toBe('count:1');
  });

  it('transforms placement (4)', () => {
    expect(
      pipe.transform({
        placement: {
          label: 'foo'
        }
      })
    ).toBe('label:foo');
  });

  it('transforms placement (5)', () => {
    expect(
      pipe.transform({
        placement: {
          host_pattern: 'abc.ceph.xyz.com'
        }
      })
    ).toBe('abc.ceph.xyz.com');
  });

  it('transforms placement (6)', () => {
    expect(
      pipe.transform({
        placement: {
          count: 2,
          hosts: ['mon0', 'mgr0']
        }
      })
    ).toBe('mon0;mgr0;count:2');
  });
});
