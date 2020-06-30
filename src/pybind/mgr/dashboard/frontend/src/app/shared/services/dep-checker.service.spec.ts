import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { NgbModalModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { OrchestratorService } from '../api/orchestrator.service';
import { DepCheckerService } from './dep-checker.service';

describe('DepCheckerService', () => {
  configureTestBed({
    providers: [DepCheckerService, OrchestratorService],
    imports: [HttpClientTestingModule, NgbModalModule]
  });

  it('should be created', () => {
    const service: DepCheckerService = TestBed.inject(DepCheckerService);
    expect(service).toBeTruthy();
  });
});
