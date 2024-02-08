import { TestBed } from '@angular/core/testing';

import { CephfsSubvolumeService } from './cephfs-subvolume.service';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CephfsSubvolumeService', () => {
  let service: CephfsSubvolumeService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [CephfsSubvolumeService]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(CephfsSubvolumeService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call get', () => {
    service.get('testFS').subscribe();
    const req = httpTesting.expectOne('api/cephfs/subvolume/testFS?group_name=&info=true');
    expect(req.request.method).toBe('GET');
  });

  it('should call remove', () => {
    service.remove('testFS', 'testSubvol').subscribe();
    const req = httpTesting.expectOne(
      'api/cephfs/subvolume/testFS?subvol_name=testSubvol&group_name=&retain_snapshots=false'
    );
    expect(req.request.method).toBe('DELETE');
  });

  it('should call getSnapshots', () => {
    service.getSnapshots('testFS', 'testSubvol').subscribe();
    const req = httpTesting.expectOne(
      'api/cephfs/subvolume/snapshot/testFS/testSubvol?group_name='
    );
    expect(req.request.method).toBe('GET');
  });

  it('should call createSnapshot', () => {
    service.createSnapshot('testFS', 'testSnap', 'testSubvol').subscribe();
    const req = httpTesting.expectOne('api/cephfs/subvolume/snapshot/');
    expect(req.request.method).toBe('POST');
  });
});
