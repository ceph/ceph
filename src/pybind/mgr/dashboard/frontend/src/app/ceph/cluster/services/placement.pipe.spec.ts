import { TestBed } from '@angular/core/testing';
import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { PlacementPipe } from './placement.pipe';

describe('PlacementPipe', () => {
  let pipe: PlacementPipe;

  configureTestBed({
    providers: [i18nProviders]
  });

  beforeEach(() => {
    const i18n = TestBed.get(I18n);
    pipe = new PlacementPipe(i18n);
  });

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
          host_pattern: '*'
        }
      })
    ).toBe('*');
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
