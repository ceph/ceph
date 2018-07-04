import { HttpClientModule } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { inject, TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { RbdMirroringService } from './rbd-mirroring.service';

describe('RbdMirroringService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [RbdMirroringService],
      imports: [HttpClientTestingModule, HttpClientModule]
    });
  });

  it('should be created', inject([RbdMirroringService], (service: RbdMirroringService) => {
    expect(service).toBeTruthy();
  }));
});
