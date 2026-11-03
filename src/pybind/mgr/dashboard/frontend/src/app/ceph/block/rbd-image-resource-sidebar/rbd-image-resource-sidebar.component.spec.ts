import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { Subject } from 'rxjs';

import { RbdImageResourceSidebarComponent } from './rbd-image-resource-sidebar.component';
import { RbdImageResourceStateService } from '../../../shared/services/rbd-image-resource-state.service';
import { ImageSpec } from '~/app/shared/models/image-spec';

describe('RbdImageResourceSidebarComponent', () => {
  let component: RbdImageResourceSidebarComponent;
  let fixture: ComponentFixture<RbdImageResourceSidebarComponent>;
  let stateServiceMock: any;
  let paramMapSubject: Subject<any>;

  beforeEach(async () => {
    paramMapSubject = new Subject();
    const activatedRouteMock = {
      paramMap: paramMapSubject.asObservable()
    };

    stateServiceMock = {
      load: jest.fn()
    };

    await TestBed.configureTestingModule({
      declarations: [RbdImageResourceSidebarComponent],
      providers: [{ provide: ActivatedRoute, useValue: activatedRouteMock }],
      schemas: [NO_ERRORS_SCHEMA]
    })
      // Crucial: Override the component's internal provider to use our mock
      .overrideComponent(RbdImageResourceSidebarComponent, {
        set: {
          providers: [{ provide: RbdImageResourceStateService, useValue: stateServiceMock }]
        }
      })
      .compileComponents();
  });

  beforeEach(() => {
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

      // Verify the generated routes
      expect(component.sidebarItems[0].route).toEqual(['/block/rbd', testRoute, 'overview']);
      expect(component.sidebarItems[1].route).toEqual(['/block/rbd', testRoute, 'snapshots']);
      expect(component.sidebarItems[2].route).toEqual(['/block/rbd', testRoute, 'configuration']);
      expect(component.sidebarItems[3].route).toEqual(['/block/rbd', testRoute, 'performance']);

      // Verify the state service was told to load the route
      expect(stateServiceMock.load).toHaveBeenCalledWith(testRoute);
    });
  });

  describe('Fallback Image Name Generation', () => {
    it('should parse a valid image spec string', () => {
      const testRoute = 'test-pool%2Ftest-image';

      // Mock ImageSpec.fromString to return a successful parsed object
      jest.spyOn(ImageSpec, 'fromString').mockReturnValue({
        imageName: 'test-image'
      } as any);

      paramMapSubject.next({ get: () => testRoute });

      expect(ImageSpec.fromString).toHaveBeenCalledWith('test-pool/test-image'); // URL decoded
      expect(component.imageName).toBe('test-image');
    });

    it('should fallback to the raw route string if parsing fails', () => {
      const invalidRoute = 'malformed-spec-string';

      // Force ImageSpec.fromString to throw an error simulating a parsing failure
      jest.spyOn(ImageSpec, 'fromString').mockImplementation(() => {
        throw new Error('Invalid format');
      });

      paramMapSubject.next({ get: () => invalidRoute });

      // Component should catch the error and fallback to the raw string
      expect(component.imageName).toBe(invalidRoute);
    });
  });

  it('should unsubscribe on destroy', () => {
    const subSpy = jest.spyOn(component['sub'], 'unsubscribe');
    component.ngOnDestroy();
    expect(subSpy).toHaveBeenCalled();
  });
});
