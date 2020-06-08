import { TestBed } from '@angular/core/testing';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { Copy2ClipboardButtonDirective } from './copy2clipboard-button.directive';

describe('Copy2clipboardButtonDirective', () => {
  configureTestBed({
    providers: [i18nProviders]
  });

  it('should create an instance', () => {
    const i18n = TestBed.inject(I18n);
    const directive = new Copy2ClipboardButtonDirective(null, null, null, i18n);
    expect(directive).toBeTruthy();
  });
});
