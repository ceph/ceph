import { HttpClient } from '@angular/common/http';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { of as observableOf } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { RbdMirroringService } from './rbd-mirroring.service';

describe('RbdMirroringService', () => {
  let service: RbdMirroringService;

  const summary = {
    status: 0,
    content_data: {
      daemons: [],
      pools: [],
      image_error: [],
      image_syncing: [],
      image_ready: []
    }
  };

  const httpClientSpy = {
    get: () => observableOf(summary)
  };

  configureTestBed({
    providers: [RbdMirroringService, { provide: HttpClient, useValue: httpClientSpy }]
  });

  beforeEach(() => {
    service = TestBed.get(RbdMirroringService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call refresh', fakeAsync(() => {
    let result = false;
    service.refresh();
    service.subscribe(() => {
      result = true;
    });
    tick(30000);
    spyOn(service, 'refresh').and.callFake(() => true);
    tick(30000);
    expect(result).toEqual(true);
  }));
});
