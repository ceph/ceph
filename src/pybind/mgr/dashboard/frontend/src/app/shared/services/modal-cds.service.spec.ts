import { TestBed } from '@angular/core/testing';

import { ModalCdsService } from './modal-cds.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ModalModule } from 'carbon-components-angular';

describe('ModalCdsService', () => {
  let service: ModalCdsService;

  configureTestBed({
    providers: [ModalCdsService],
    imports: [ModalModule]
  });

  beforeEach(() => {
    service = TestBed.inject(ModalCdsService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
