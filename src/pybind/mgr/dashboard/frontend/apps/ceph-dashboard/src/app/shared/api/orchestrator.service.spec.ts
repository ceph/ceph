import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { OrchestratorService } from './orchestrator.service';

describe('OrchestratorService', () => {
  let service: OrchestratorService;
  let httpTesting: HttpTestingController;
  const uiApiPath = 'ui-api/orchestrator';

  configureTestBed({
    providers: [OrchestratorService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(OrchestratorService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call status', () => {
    service.status().subscribe();
    const req = httpTesting.expectOne(`${uiApiPath}/status`);
    expect(req.request.method).toBe('GET');
  });
});
