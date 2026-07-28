import { configureTestBed } from '~/testing/unit-test-helper';
import { TestBed } from '@angular/core/testing';

import { ComboBoxService } from './combo-box.service';

describe('ComboBoxService', () => {
  let service: ComboBoxService;

  configureTestBed({});

  beforeEach(() => {
    service = TestBed.inject(ComboBoxService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
