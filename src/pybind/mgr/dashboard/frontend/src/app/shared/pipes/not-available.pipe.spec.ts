import { TestBed } from '@angular/core/testing';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { NotAvailablePipe } from './not-available.pipe';

describe('NotAvailablePipe', () => {
  let pipe: NotAvailablePipe;

  configureTestBed({
    providers: [i18nProviders]
  });

  beforeEach(() => {
    const i18n = TestBed.inject(I18n);
    pipe = new NotAvailablePipe(i18n);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms not available', () => {
    expect(pipe.transform('')).toBe('n/a');
  });

  it('transforms number', () => {
    expect(pipe.transform(0)).toBe(0);
    expect(pipe.transform(1)).toBe(1);
  });
});
