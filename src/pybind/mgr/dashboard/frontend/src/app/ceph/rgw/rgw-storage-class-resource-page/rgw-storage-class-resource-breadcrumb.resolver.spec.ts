import { TestBed } from '@angular/core/testing';
import { ActivatedRouteSnapshot } from '@angular/router';
import { RgwStorageClassResourceBreadcrumbResolver } from './rgw-storage-class-resource-breadcrumb.resolver';

describe('RgwStorageClassResourceBreadcrumbResolver', () => {
  let resolver: RgwStorageClassResourceBreadcrumbResolver;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [RgwStorageClassResourceBreadcrumbResolver]
    });

    resolver = TestBed.inject(RgwStorageClassResourceBreadcrumbResolver);

    // Mock the inherited getFullPath method so we don't have to build a full route tree
    jest.spyOn(resolver, 'getFullPath').mockReturnValue('/mock/full/path');
  });

  it('should be created', () => {
    expect(resolver).toBeTruthy();
  });

  describe('resolve', () => {
    it('should extract storage_class from parent params first', () => {
      // Arrange
      const route = {
        parent: {
          params: { storage_class: 'parent-sc-name' }
        },
        params: { storage_class: 'child-sc-name' } // Should be ignored
      } as unknown as ActivatedRouteSnapshot;

      // Act
      const result = resolver.resolve(route);

      // Assert
      expect(result).toEqual([{ text: 'parent-sc-name', path: '/mock/full/path' }]);
      expect(resolver.getFullPath).toHaveBeenCalledWith(route);
    });

    it('should extract storage_class from route params if parent params are missing', () => {
      // Arrange
      const route = {
        parent: {
          params: {} // Empty parent params
        },
        params: { storage_class: 'child-sc-name' }
      } as unknown as ActivatedRouteSnapshot;

      // Act
      const result = resolver.resolve(route);

      // Assert
      expect(result).toEqual([{ text: 'child-sc-name', path: '/mock/full/path' }]);
      expect(resolver.getFullPath).toHaveBeenCalledWith(route);
    });

    it('should extract storage_class from route params if parent is completely null', () => {
      // Arrange
      const route = {
        parent: null,
        params: { storage_class: 'child-sc-name' }
      } as unknown as ActivatedRouteSnapshot;

      // Act
      const result = resolver.resolve(route);

      // Assert
      expect(result).toEqual([{ text: 'child-sc-name', path: '/mock/full/path' }]);
    });

    it('should return an empty string if storage_class is missing entirely', () => {
      // Arrange
      const route = {
        parent: { params: {} },
        params: {}
      } as unknown as ActivatedRouteSnapshot;

      // Act
      const result = resolver.resolve(route);

      // Assert
      expect(result).toEqual([{ text: '', path: '/mock/full/path' }]);
    });
  });
});
