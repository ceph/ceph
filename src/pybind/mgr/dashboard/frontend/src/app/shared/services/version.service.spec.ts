import { HttpClient } from '@angular/common/http';
import { TestBed, inject } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { of as observableOf, Subscriber } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';

import { VersionService } from './version.service';

describe('VersionService', () => {
  let versionService: VersionService;

  const version = {
    raw: "ceph version 14.0.0-4116-gbd6b4b4 (bd6b4b46e5932d8827687e1e8ed624293daa5d60) nautilus (dev)",
    hash: "bd6b4b46e5932d8827687e1e8ed624293daa5d60",
    name: "nautilus",
    number: "14.0.0-4116-gbd6b4b4"
  };

  const httpClientSpy = {
    get: () => observableOf(version)
  };

  configureTestBed({
    imports: [RouterTestingModule],
    providers: [
      VersionService,
      { provide: HttpClient, useValue: httpClientSpy }
    ]
  });

  beforeEach(() => {
    versionService = TestBed.get(VersionService);
  });

  it('should be created', () => {
    expect(versionService).toBeTruthy();
  });

  it('should call getCurrentVersion', () => {
    expect(versionService.getCurrentVersion()).toEqual(version);
  });
});
