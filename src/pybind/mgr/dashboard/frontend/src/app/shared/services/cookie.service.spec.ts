import { TestBed } from '@angular/core/testing';

import { CookiesService } from './cookie.service';

describe('CookieService', () => {
  let service: CookiesService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(CookiesService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
