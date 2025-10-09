import { TestBed } from '@angular/core/testing';

import { WizardStepsService } from './wizard-steps.service';

describe('WizardStepsService', () => {
  let service: WizardStepsService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(WizardStepsService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
