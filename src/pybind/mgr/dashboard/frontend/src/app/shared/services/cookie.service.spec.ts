import { configureTestBed } from '~/testing/unit-test-helper';
import { TestBed } from '@angular/core/testing';

import { CookiesService } from './cookie.service';

describe('CookieService', () => {
  let service: CookiesService;

  configureTestBed({});

  beforeEach(() => {
    service = TestBed.inject(CookiesService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
