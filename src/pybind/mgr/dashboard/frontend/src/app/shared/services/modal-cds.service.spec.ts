import { TestBed } from '@angular/core/testing';

import { ModalCdsService } from './modal-cds.service';

describe('ModalCdsService', () => {
  let service: ModalCdsService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ModalCdsService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
