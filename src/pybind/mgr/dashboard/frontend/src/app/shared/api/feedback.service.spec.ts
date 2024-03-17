import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { FeedbackService } from './feedback.service';

describe('FeedbackService', () => {
  let service: FeedbackService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [FeedbackService]
  });

  beforeEach(() => {
    service = TestBed.inject(FeedbackService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call checkAPIKey', () => {
    service.isKeyExist().subscribe();
    const req = httpTesting.expectOne('ui-api/feedback/api_key/exist');
    expect(req.request.method).toBe('GET');
  });

  it('should call createIssue to create issue tracker', () => {
    service.createIssue('dashboard', 'bug', 'test', 'test', '').subscribe();
    const req = httpTesting.expectOne('api/feedback');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({
      api_key: '',
      description: 'test',
      project: 'dashboard',
      subject: 'test',
      tracker: 'bug'
    });
  });

  it('should call list to get feedback list', () => {
    service.list().subscribe();
    const req = httpTesting.expectOne('api/feedback');
    expect(req.request.method).toBe('GET');
  });
});
