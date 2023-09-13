import { TestBed } from '@angular/core/testing';
import { DomSanitizer } from '@angular/platform-browser';

import { SanitizeHtmlPipe } from '~/app/shared/pipes/sanitize-html.pipe';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('SanitizeHtmlPipe', () => {
  let pipe: SanitizeHtmlPipe;
  let domSanitizer: DomSanitizer;

  configureTestBed({
    providers: [DomSanitizer]
  });

  beforeEach(() => {
    domSanitizer = TestBed.inject(DomSanitizer);
    pipe = new SanitizeHtmlPipe(domSanitizer);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  // There is no way to inject a working DomSanitizer in unit tests,
  // so it is not possible to test the `transform` method.
});
