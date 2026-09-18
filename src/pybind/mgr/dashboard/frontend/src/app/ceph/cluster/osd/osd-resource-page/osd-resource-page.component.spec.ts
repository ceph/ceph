import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { of, BehaviorSubject } from 'rxjs';

import { OsdResourcePageComponent } from './osd-resource-page.component';
import { OsdService } from '~/app/shared/api/osd.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Osd } from '~/app/shared/models/osd.model';

describe('OsdResourcePageComponent', () => {
  let component: OsdResourcePageComponent;
  let fixture: ComponentFixture<OsdResourcePageComponent>;

  let osdServiceMock: any;
  let formatterMock: any;
  let paramMapSubject: BehaviorSubject<any>;

  const mockOsd: Osd = {
    id: 1,
    host: { name: 'host-1' },
    tree: { device_class: 'ssd' },
    in: 1,
    up: 1,
    state: ['noup', 'exists'],
    stats: {
      stat_bytes: 10000,
      stat_bytes_used: 2000,
      numpg: 100,
      op_r: 50.55,
      op_w: 10.11
    },
    stats_history: {
      op_in_bytes: [[1620000000, 1024]], // Format: [timestamp, value]
      op_out_bytes: [2048] // Format: flat value
    }
  } as unknown as Osd;

  beforeEach(async () => {
    paramMapSubject = new BehaviorSubject(convertToParamMap({ id: '1' }));

    osdServiceMock = {
      getList: jest.fn().mockReturnValue({ observable: of([mockOsd]) }),
      getFlags: jest.fn().mockReturnValue(of(['sortbitwise', 'noout', 'nodeep-scrub'])),
      getDetails: jest.fn().mockReturnValue(
        of({
          osd_map: { test_map: true },
          osd_metadata: { test_meta: true }
        })
      )
    };

    formatterMock = {
      formatToBinary: jest.fn((val) => (val !== null ? `${val} B` : '-'))
    };

    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      declarations: [OsdResourcePageComponent],
      providers: [
        { provide: OsdService, useValue: osdServiceMock },
        { provide: FormatterService, useValue: formatterMock },
        {
          provide: AuthStorageService,
          useValue: { getPermissions: () => ({ grafana: { read: true } }) }
        },
        {
          provide: ActivatedRoute,
          useValue: {
            parent: {
              paramMap: paramMapSubject.asObservable(),
              snapshot: { paramMap: convertToParamMap({ id: '1' }) }
            },
            paramMap: of(convertToParamMap({})),
            snapshot: { data: { section: 'overview' } }
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdResourcePageComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('ngOnInit Data Loading (Route ID)', () => {
    it('should load data from route ID and populate view models correctly', fakeAsync(() => {
      fixture.detectChanges();
      tick();

      expect(osdServiceMock.getList).toHaveBeenCalled();
      expect(osdServiceMock.getFlags).toHaveBeenCalled();
      expect(osdServiceMock.getDetails).toHaveBeenCalledWith(1);

      // Verify basic OSD assignments
      expect(component.osdId).toBe(1);
      expect(component.osd?.id).toBe(1);
      expect(component.osdMap).toEqual({ test_map: true });
      expect(component.osdMetadata).toEqual({ test_meta: true });

      // Verify states calculation (in=1, up=1)
      expect(component.osd?.collectedStates).toEqual(['in', 'up']);

      // Verify flag filtering
      // 'sortbitwise' is in disabledFlags and should be filtered out from cluster flags.
      // 'noout' is an indivFlag.
      expect(component.osd?.cdClusterFlags).toEqual(['noout', 'nodeep-scrub']);
      expect(component.osd?.cdIndivFlags).toEqual(['noup']);

      // Verify Capacity Overview
      expect(component.capacityOverviewModel.usageTotal).toBe(10000);
      expect(component.capacityOverviewModel.usageUsed).toBe(2000);
      expect(component.capacityOverviewModel.usagePercent).toBe('20%');
      expect(component.capacityOverviewModel.totalCapacity).toBe('10000 B');

      // Verify IO Overview
      expect(component.ioOverviewModel.readOps).toBe('50.6/s');
      expect(component.ioOverviewModel.writeOps).toBe('10.1/s');
      expect(component.ioOverviewModel.readBytes).toBe('2048 B');
      expect(component.ioOverviewModel.writeBytes).toBe('1024 B');
    }));

    it('should reset view model if OSD ID is not found in the list', fakeAsync(() => {
      osdServiceMock.getList.mockReturnValue({ observable: of([]) });

      fixture.detectChanges();
      tick();

      expect(component.osdId).toBeNull();
      expect(component.osd).toBeNull();
      expect(component.osdOverviewFields).toEqual([]);
    }));
  });

  describe('State Collection Logic', () => {
    it('should correctly identify OUT and DOWN states', fakeAsync(() => {
      const downOsd = { ...mockOsd, in: 0, up: 0, state: ['exists'] } as unknown as Osd;
      osdServiceMock.getList.mockReturnValue({ observable: of([downOsd]) });

      fixture.detectChanges();
      tick();

      expect(component.osd?.collectedStates).toEqual(['out', 'down']);
    }));

    it('should correctly identify OUT and DESTROYED states', fakeAsync(() => {
      const destroyedOsd = { ...mockOsd, in: 0, up: 0, state: ['destroyed'] } as unknown as Osd;
      osdServiceMock.getList.mockReturnValue({ observable: of([destroyedOsd]) });

      fixture.detectChanges();
      tick();

      expect(component.osd?.collectedStates).toEqual(['out', 'destroyed']);
    }));
  });

  describe('Refresh', () => {
    it('should fetch details and update maps when refresh is called with a valid ID', fakeAsync(() => {
      component.osdId = 1;
      component.refresh();
      tick();

      expect(osdServiceMock.getDetails).toHaveBeenCalledWith(1);
      expect(component.osdMap).toEqual({ test_map: true });
      expect(component.osdMetadata).toEqual({ test_meta: true });
    }));

    it('should safely do nothing if osdId is missing during refresh', () => {
      component.osdId = null;
      component.refresh();
      expect(osdServiceMock.getDetails).not.toHaveBeenCalled();
    });
  });

  describe('ngOnDestroy', () => {
    it('should unsubscribe from observables to prevent memory leaks', () => {
      const subSpy = jest.spyOn((component as any).sub, 'unsubscribe');
      component.ngOnDestroy();
      expect(subSpy).toHaveBeenCalled();
    });
  });
});
