import { TestBed } from '@angular/core/testing';
import { of, throwError } from 'rxjs';
import { take } from 'rxjs/operators';

import { CephfsResourceStateService } from './cephfs-resource-state.service';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsDetail } from '~/app/shared/models/cephfs.model';

describe('CephfsResourceStateService', () => {
  let service: CephfsResourceStateService;
  let cephfsServiceMock: { list: jest.Mock };

  const mockFilesystems: CephfsDetail[] = [
    { id: 1, cephfs: { name: 'cephfs-a' } } as CephfsDetail,
    { id: 2, cephfs: { name: 'cephfs-b' } } as CephfsDetail,
    { id: 3, cephfs: { name: 'cephfs-c' } } as CephfsDetail
  ];

  beforeEach(() => {
    // Set up the API mock
    cephfsServiceMock = {
      list: jest.fn().mockReturnValue(of(mockFilesystems))
    };

    TestBed.configureTestingModule({
      providers: [
        CephfsResourceStateService,
        { provide: CephfsService, useValue: cephfsServiceMock }
      ]
    });

    service = TestBed.inject(CephfsResourceStateService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('load()', () => {
    it.each([
      ['empty string', ''],
      ['not a number', 'abc'],
      ['zero', '0'],
      ['negative number', '-5'],
      ['null', null as unknown as string],
      ['undefined', undefined as unknown as string]
    ])('should emit null and not call API if fsId is %s', (_desc, invalidId) => {
      service.load(invalidId);

      // Verify state is emitted as null
      service.filesystem$.pipe(take(1)).subscribe((fs) => {
        expect(fs).toBeNull();
      });

      // Verify API was completely bypassed
      expect(cephfsServiceMock.list).not.toHaveBeenCalled();
    });

    it('should fetch data from the API and emit the matching filesystem', (done) => {
      service.load('2');

      service.filesystem$.pipe(take(1)).subscribe((fs) => {
        expect(fs).toEqual(mockFilesystems[1]);
        expect(cephfsServiceMock.list).toHaveBeenCalledTimes(1);
        done();
      });
    });

    it('should emit null if the API returns data but the requested ID is not found', (done) => {
      service.load('99'); // ID 99 does not exist in mockFilesystems

      service.filesystem$.pipe(take(1)).subscribe((fs) => {
        expect(fs).toBeNull();
        expect(cephfsServiceMock.list).toHaveBeenCalledTimes(1);
        done();
      });
    });

    it('should emit null if the API returns an empty or null array', (done) => {
      cephfsServiceMock.list.mockReturnValue(of(null));

      service.load('1');

      service.filesystem$.pipe(take(1)).subscribe((fs) => {
        expect(fs).toBeNull();
        expect(cephfsServiceMock.list).toHaveBeenCalledTimes(1);
        done();
      });
    });

    it('should emit null if the API call throws an error', (done) => {
      // Simulate an API failure
      cephfsServiceMock.list.mockReturnValue(throwError(() => new Error('API Error')));

      service.load('1');

      service.filesystem$.pipe(take(1)).subscribe((fs) => {
        expect(fs).toBeNull();
        expect(cephfsServiceMock.list).toHaveBeenCalledTimes(1);
        done();
      });
    });
  });
});
