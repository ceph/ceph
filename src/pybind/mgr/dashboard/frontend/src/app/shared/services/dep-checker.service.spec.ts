import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { BsModalService, ModalModule } from 'ngx-bootstrap/modal';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { OrchestratorService } from '../api/orchestrator.service';
import { DepCheckerService } from './dep-checker.service';

describe('DepCheckerService', () => {
  configureTestBed({
    providers: [BsModalService, DepCheckerService, OrchestratorService],
    imports: [HttpClientTestingModule, ModalModule.forRoot()]
  });

  it('should be created', () => {
    const service: DepCheckerService = TestBed.inject(DepCheckerService);
    expect(service).toBeTruthy();
  });
});
