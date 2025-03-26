import { TestBed } from '@angular/core/testing';

import { ComboBoxService } from './combo-box.service';

describe('ComboBoxService', () => {
  let service: ComboBoxService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ComboBoxService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
