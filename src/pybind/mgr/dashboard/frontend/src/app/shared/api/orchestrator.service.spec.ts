import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { OrchestratorService } from './orchestrator.service';

describe('OrchestratorService', () => {
  let service: OrchestratorService;
  let httpTesting: HttpTestingController;
  const apiPath = 'api/orchestrator';

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
    const req = httpTesting.expectOne(`${apiPath}/status`);
    expect(req.request.method).toBe('GET');
  });

  it('should call inventoryList with arguments', () => {
    const inventoryPath = `${apiPath}/inventory`;
    const tests: { args: any[]; expectedUrl: any }[] = [
      {
        args: [],
        expectedUrl: inventoryPath
      },
      {
        args: ['host0'],
        expectedUrl: `${inventoryPath}?hostname=host0`
      },
      {
        args: [undefined, true],
        expectedUrl: `${inventoryPath}?refresh=true`
      },
      {
        args: ['host0', true],
        expectedUrl: `${inventoryPath}?hostname=host0&refresh=true`
      }
    ];

    for (const test of tests) {
      service.inventoryList(...test.args).subscribe();
      const req = httpTesting.expectOne(test.expectedUrl);
      expect(req.request.method).toBe('GET');
    }
  });
});
