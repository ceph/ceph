import { TestBed } from '@angular/core/testing';
import { of, throwError } from 'rxjs';
import { take } from 'rxjs/operators';

import { RbdImageResourceStateService } from './rbd-image-resource-state.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { ImageSpec } from '~/app/shared/models/image-spec';
import { RbdFormModel } from '../../ceph/block/rbd-form/rbd-form.model';

describe('RbdImageResourceStateService', () => {
  let service: RbdImageResourceStateService;
  let rbdServiceSpy: any;

  beforeEach(() => {
    rbdServiceSpy = {
      get: jest.fn()
    };

    TestBed.configureTestingModule({
      providers: [RbdImageResourceStateService, { provide: RbdService, useValue: rbdServiceSpy }]
    });

    service = TestBed.inject(RbdImageResourceStateService);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('load()', () => {
    it('should emit null if imageSpecRoute is empty', (done) => {
      service.image$.pipe(take(1)).subscribe((image) => {
        expect(image).toBeNull();
        done();
      });

      service.load('');
    });

    it('should emit null if imageSpecRoute is null or undefined', (done) => {
      service.image$.pipe(take(1)).subscribe((image) => {
        expect(image).toBeNull();
        done();
      });

      service.load(null as any);
    });

    it('should emit null if parsing ImageSpec fails', (done) => {
      // Force the static method to throw an error
      jest.spyOn(ImageSpec, 'fromString').mockImplementation(() => {
        throw new Error('Invalid format');
      });

      service.image$.pipe(take(1)).subscribe((image) => {
        expect(image).toBeNull();
        expect(rbdServiceSpy.get).not.toHaveBeenCalled();
        done();
      });

      service.load('invalid-format-string');
    });

    it('should call rbdService.get and emit the image on success', (done) => {
      const mockImageSpec = new ImageSpec('test-pool', 'test-namespace', 'test-image');
      const mockResponse: Partial<RbdFormModel> = { name: 'test-image', pool_name: 'test-pool' };

      jest.spyOn(ImageSpec, 'fromString').mockReturnValue(mockImageSpec);
      rbdServiceSpy.get.mockReturnValue(of(mockResponse));

      service.image$.pipe(take(1)).subscribe((image) => {
        expect(ImageSpec.fromString).toHaveBeenCalledWith('test-pool/test-image'); // Decoded URL string
        expect(rbdServiceSpy.get).toHaveBeenCalledWith(mockImageSpec);
        expect(image).toEqual(mockResponse);
        done();
      });

      service.load('test-pool%2Ftest-image'); // URL Encoded string
    });

    it('should emit null if rbdService.get returns an error', (done) => {
      const mockImageSpec = new ImageSpec('test-pool', 'test-namespace', 'test-image');

      jest.spyOn(ImageSpec, 'fromString').mockReturnValue(mockImageSpec);
      // Simulate an HTTP error from the API
      rbdServiceSpy.get.mockReturnValue(throwError(() => new Error('API Error')));

      service.image$.pipe(take(1)).subscribe((image) => {
        expect(rbdServiceSpy.get).toHaveBeenCalledWith(mockImageSpec);
        expect(image).toBeNull();
        done();
      });

      service.load('test-pool%2Ftest-image');
    });
  });
});
