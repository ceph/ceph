import { TestBed } from '@angular/core/testing';

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { OrchestratorService } from './orchestrator.service';

describe('OrchestratorService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  configureTestBed({
    providers: [OrchestratorService, i18nProviders],
    imports: [HttpClientTestingModule]
  });

  it('should be created', () => {
    const service: OrchestratorService = TestBed.get(OrchestratorService);
    expect(service).toBeTruthy();
  });
});
