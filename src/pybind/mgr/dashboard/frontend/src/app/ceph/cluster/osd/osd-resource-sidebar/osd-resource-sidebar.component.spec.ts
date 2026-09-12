import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { BehaviorSubject, of } from 'rxjs';

import { OsdResourceSidebarComponent } from './osd-resource-sidebar.component';
import { OsdService } from '~/app/shared/api/osd.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Osd } from '~/app/shared/models/osd.model';
import { Permissions } from '~/app/shared/models/permissions';

describe('OsdResourceSidebarComponent', () => {
  let component: OsdResourceSidebarComponent;
  let fixture: ComponentFixture<OsdResourceSidebarComponent>;
  let osdServiceMock: any;
  let authStorageServiceMock: any;
  let paramMapSubject: BehaviorSubject<any>;

  beforeEach(async () => {
    paramMapSubject = new BehaviorSubject(convertToParamMap({ id: '1' }));

    osdServiceMock = {
      getList: jest.fn().mockReturnValue({ observable: of([]) }),
      getFlags: jest.fn().mockReturnValue(of([]))
    };

    authStorageServiceMock = {
      getPermissions: jest.fn().mockReturnValue({ grafana: { read: false } } as Permissions)
    };

    await TestBed.configureTestingModule({
      declarations: [OsdResourceSidebarComponent],
      providers: [
        { provide: OsdService, useValue: osdServiceMock },
        { provide: AuthStorageService, useValue: authStorageServiceMock },
        {
          provide: ActivatedRoute,
          useValue: { paramMap: paramMapSubject.asObservable() }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdResourceSidebarComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('title getter', () => {
    it('should return formatted title when osdId is present', () => {
      fixture.detectChanges(); // id is '1'
      expect(component.title).toBe('OSD 1');
    });

    it('should return empty string when osdId is empty', () => {
      paramMapSubject.next(convertToParamMap({}));
      fixture.detectChanges();
      expect(component.title).toBe('');
    });
  });

  describe('buildSidebarItems', () => {
    it('should build sidebar without Grafana performance item if lacking permissions', () => {
      authStorageServiceMock.getPermissions.mockReturnValue({
        grafana: { read: false }
      } as Permissions);
      fixture = TestBed.createComponent(OsdResourceSidebarComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();

      expect(component.sidebarItems.length).toBe(4);
      expect(component.sidebarItems.map((item) => item.label)).not.toContain('Performance');
    });

    it('should build sidebar with Grafana performance item if permission exists', () => {
      authStorageServiceMock.getPermissions.mockReturnValue({
        grafana: { read: true }
      } as Permissions);
      fixture = TestBed.createComponent(OsdResourceSidebarComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();

      expect(component.sidebarItems.length).toBe(5);
      expect(component.sidebarItems.map((item) => item.label)).toContain('Performance');
      expect(component.sidebarItems[4].route).toEqual(['/osd/view', '1', 'performance']);
    });
  });

  describe('loadHeaderTags', () => {
    it('should exit early and clear tags if osdId is not a valid number', fakeAsync(() => {
      paramMapSubject.next(convertToParamMap({ id: 'abc' }));
      fixture.detectChanges();
      tick();

      expect(osdServiceMock.getList).not.toHaveBeenCalled();
      expect(component.headerTags).toEqual([]);
    }));

    it('should clear tags if the OSD ID is valid but not found in the list', fakeAsync(() => {
      const mockOsds = [{ id: 99 } as Osd]; // Doesn't match ID 1
      osdServiceMock.getList.mockReturnValue({ observable: of(mockOsds) });

      fixture.detectChanges();
      tick();

      expect(component.headerTags).toEqual([]);
    }));

    it('should collect and format header tags correctly for an IN/UP OSD', fakeAsync(() => {
      const mockOsds = [
        {
          id: 1,
          in: 1,
          up: 1,
          tree: { device_class: 'ssd' },
          state: ['noup', 'exists']
        } as unknown as Osd
      ];
      const mockClusterFlags = ['nodeep-scrub', 'sortbitwise']; // 'sortbitwise' should be filtered out

      osdServiceMock.getList.mockReturnValue({ observable: of(mockOsds) });
      osdServiceMock.getFlags.mockReturnValue(of(mockClusterFlags));

      fixture.detectChanges();
      tick();

      // Expected tags: device_class (ssd), states (in, up), clusterFlags (nodeep-scrub), individualFlags (noup)
      expect(component.headerTags).toEqual(['ssd', 'in', 'up', 'nodeep-scrub', 'noup']);
    }));

    it('should collect and format header tags correctly for an OUT/DOWN OSD with no class', fakeAsync(() => {
      const mockOsds = [
        {
          id: 1,
          in: 0,
          up: 0,
          state: ['exists']
        } as unknown as Osd
      ];

      osdServiceMock.getList.mockReturnValue({ observable: of(mockOsds) });
      osdServiceMock.getFlags.mockReturnValue(of([]));

      fixture.detectChanges();
      tick();

      // Expected tags: states (out, down)
      expect(component.headerTags).toEqual(['out', 'down']);
    }));

    it('should report destroyed state if DOWN and state includes destroyed', fakeAsync(() => {
      const mockOsds = [
        {
          id: 1,
          in: 0,
          up: 0,
          state: ['destroyed', 'noout']
        } as unknown as Osd
      ];

      osdServiceMock.getList.mockReturnValue({ observable: of(mockOsds) });
      osdServiceMock.getFlags.mockReturnValue(of([]));

      fixture.detectChanges();
      tick();

      // Expected tags: states (out, destroyed), individual flags (noout)
      expect(component.headerTags).toEqual(['out', 'destroyed', 'noout']);
    }));
  });

  describe('ngOnDestroy', () => {
    it('should unsubscribe from observables', () => {
      fixture.detectChanges();
      const subSpy = jest.spyOn((component as any).sub, 'unsubscribe');

      component.ngOnDestroy();

      expect(subSpy).toHaveBeenCalled();
    });
  });
});
