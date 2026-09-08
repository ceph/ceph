import { TestBed } from '@angular/core/testing';
import { ActivatedRouteSnapshot } from '@angular/router';

import { RbdImageResourceBreadcrumbResolver } from './rbd-image-resource-breadcrumb.resolver';
import { ImageSpec } from '~/app/shared/models/image-spec';

describe('RbdImageResourceBreadcrumbResolver', () => {
  let resolver: RbdImageResourceBreadcrumbResolver;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [RbdImageResourceBreadcrumbResolver]
    });

    resolver = TestBed.inject(RbdImageResourceBreadcrumbResolver);
    jest.spyOn(resolver as any, 'getFullPath').mockReturnValue('/mock/full/path');
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('should be created', () => {
    expect(resolver).toBeTruthy();
  });

  it('should resolve breadcrumb with parsed image name from URL decoded spec', () => {
    const mockRoute = {
      paramMap: {
        get: jest.fn().mockReturnValue('test-pool%2Ftest-image')
      }
    } as unknown as ActivatedRouteSnapshot;

    jest.spyOn(ImageSpec, 'fromString').mockReturnValue({
      imageName: 'test-image'
    } as any);
    const result = resolver.resolve(mockRoute);
    expect(mockRoute.paramMap.get).toHaveBeenCalledWith('image_spec');
    expect(ImageSpec.fromString).toHaveBeenCalledWith('test-pool/test-image');
    expect(result).toEqual([{ text: 'test-image', path: '/mock/full/path' }]);
  });

  it('should fallback to the raw route string if ImageSpec parsing fails', () => {
    const invalidRouteParam = 'malformed-spec-string';
    const mockRoute = {
      paramMap: {
        get: jest.fn().mockReturnValue(invalidRouteParam)
      }
    } as unknown as ActivatedRouteSnapshot;

    jest.spyOn(ImageSpec, 'fromString').mockImplementation(() => {
      throw new Error('Invalid format');
    });

    const result = resolver.resolve(mockRoute);
    expect(result).toEqual([{ text: invalidRouteParam, path: '/mock/full/path' }]);
  });

  it('should return an empty text string if the route parameter is missing', () => {
    const mockRoute = {
      paramMap: {
        get: jest.fn().mockReturnValue(null)
      }
    } as unknown as ActivatedRouteSnapshot;

    jest.spyOn(ImageSpec, 'fromString');

    const result = resolver.resolve(mockRoute);
    expect(ImageSpec.fromString).not.toHaveBeenCalled();
    expect(result).toEqual([{ text: '', path: '/mock/full/path' }]);
  });
});
