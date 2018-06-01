import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { inject } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { RbdMirroringService } from './rbd-mirroring.service';

describe('RbdMirroringService', () => {
  configureTestBed({
    providers: [RbdMirroringService],
    imports: [HttpClientTestingModule, HttpClientModule]
  });

  it(
    'should be created',
    inject([RbdMirroringService], (service: RbdMirroringService) => {
      expect(service).toBeTruthy();
    })
  );
});
