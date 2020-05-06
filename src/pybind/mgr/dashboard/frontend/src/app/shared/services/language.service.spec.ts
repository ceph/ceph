import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { LanguageService } from './language.service';

describe('LanguageService', () => {
  let service: LanguageService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [LanguageService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(LanguageService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call create', () => {
    service.getLanguages().subscribe();
    const req = httpTesting.expectOne('ui-api/langs');
    expect(req.request.method).toBe('GET');
  });
});
