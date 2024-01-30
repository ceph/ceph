import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { CustomLoginBannerService } from './custom-login-banner.service';

describe('CustomLoginBannerService', () => {
  let service: CustomLoginBannerService;
  let httpTesting: HttpTestingController;
  const baseUiURL = 'ui-api/login/custom_banner';

  configureTestBed({
    providers: [CustomLoginBannerService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(CustomLoginBannerService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getBannerText', () => {
    service.getBannerText().subscribe();
    const req = httpTesting.expectOne(baseUiURL);
    expect(req.request.method).toBe('GET');
  });
});
