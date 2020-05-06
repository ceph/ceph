import { TestBed } from '@angular/core/testing';
import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { BooleanTextPipe } from './boolean-text.pipe';

describe('BooleanTextPipe', () => {
  let pipe: BooleanTextPipe;

  configureTestBed({
    providers: [i18nProviders]
  });

  beforeEach(() => {
    const i18n = TestBed.inject(I18n);
    pipe = new BooleanTextPipe(i18n);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms true', () => {
    expect(pipe.transform(true)).toEqual('Yes');
  });

  it('transforms true, alternative text', () => {
    expect(pipe.transform(true, 'foo')).toEqual('foo');
  });

  it('transforms 1', () => {
    expect(pipe.transform(1)).toEqual('Yes');
  });

  it('transforms false', () => {
    expect(pipe.transform(false)).toEqual('No');
  });

  it('transforms false, alternative text', () => {
    expect(pipe.transform(false, 'foo', 'bar')).toEqual('bar');
  });

  it('transforms 0', () => {
    expect(pipe.transform(0)).toEqual('No');
  });
});
