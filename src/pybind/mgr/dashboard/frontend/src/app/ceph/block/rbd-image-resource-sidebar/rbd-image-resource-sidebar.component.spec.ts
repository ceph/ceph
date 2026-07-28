import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Subject } from 'rxjs';

import { RbdImageResourceSidebarComponent } from './rbd-image-resource-sidebar.component';
import { RbdImageResourceStateService } from '../../../shared/services/rbd-image-resource-state.service';
import { ImageSpec } from '~/app/shared/models/image-spec';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RbdImageResourceSidebarComponent', () => {
  let component: RbdImageResourceSidebarComponent;
  let fixture: ComponentFixture<RbdImageResourceSidebarComponent>;
  let stateServiceMock: any = { load: jest.fn() };
  let paramMapSubject: Subject<any>;

  configureTestBed({
    declarations: [RbdImageResourceSidebarComponent],
    imports: [HttpClientTestingModule],
    providers: [
      {
        provide: ActivatedRoute,
        useFactory: () => ({ paramMap: paramMapSubject.asObservable() })
      }
    ],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(async () => {
    paramMapSubject = new Subject();
    stateServiceMock.load.mockClear();

    TestBed.overrideComponent(RbdImageResourceSidebarComponent, {
      set: {
        providers: [{ provide: RbdImageResourceStateService, useFactory: () => stateServiceMock }]
      }
    });

    await TestBed.compileComponents();

    fixture = TestBed.createComponent(RbdImageResourceSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('ngOnInit routing and state initialization', () => {
    it('should handle empty route param gracefully', () => {
      paramMapSubject.next({ get: () => null });

      expect(component.imageSpecRoute).toBe('');
      expect(component.imageName).toBe('');
      expect(component.sidebarItems.length).toBe(4);
      expect(stateServiceMock.load).toHaveBeenCalledWith('');
    });

    it('should build sidebar items correctly based on route param', () => {
      const testRoute = 'test-pool%2Ftest-image';
      paramMapSubject.next({ get: () => testRoute });

      expect(component.imageSpecRoute).toBe(testRoute);
      expect(component.sidebarItems.length).toBe(4);

      expect(component.sidebarItems[0].route).toEqual(['/block/rbd', testRoute, 'overview']);
      expect(component.sidebarItems[1].route).toEqual(['/block/rbd', testRoute, 'snapshots']);
      expect(component.sidebarItems[2].route).toEqual(['/block/rbd', testRoute, 'configuration']);
      expect(component.sidebarItems[3].route).toEqual(['/block/rbd', testRoute, 'performance']);

      expect(stateServiceMock.load).toHaveBeenCalledWith(testRoute);
    });
  });

  describe('Fallback Image Name Generation', () => {
    it('should parse a valid image spec string', () => {
      const testRoute = 'test-pool%2Ftest-image';

      jest.spyOn(ImageSpec, 'fromString').mockReturnValue({
        imageName: 'test-image'
      } as any);

      paramMapSubject.next({ get: () => testRoute });

      expect(ImageSpec.fromString).toHaveBeenCalledWith('test-pool/test-image');
      expect(component.imageName).toBe('test-image');
    });

    it('should fallback to the raw route string if parsing fails', () => {
      const invalidRoute = 'malformed-spec-string';

      jest.spyOn(ImageSpec, 'fromString').mockImplementation(() => {
        throw new Error('Invalid format');
      });

      paramMapSubject.next({ get: () => invalidRoute });

      expect(component.imageName).toBe(invalidRoute);
    });
  });

  it('should unsubscribe on destroy', () => {
    const subSpy = jest.spyOn(component['sub'], 'unsubscribe');
    component.ngOnDestroy();
    expect(subSpy).toHaveBeenCalled();
  });
});
