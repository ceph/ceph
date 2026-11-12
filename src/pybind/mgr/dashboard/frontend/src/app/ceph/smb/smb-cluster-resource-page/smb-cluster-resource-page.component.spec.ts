import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { BehaviorSubject, of } from 'rxjs';

import { SmbClusterResourcePageComponent } from './smb-cluster-resource-page.component';
import { SmbClusterResourceStateService } from '~/app/shared/services/smb-cluster-resource-state.service';
import { CLIENT_COMPAT, SMBCluster } from '../smb.model';
import {
  SMB_CEPHFS_CONTEXT,
  SMB_CONTEXT,
  SMB_RGW_CONTEXT,
  SmbContext
} from '~/app/shared/constants/smb.constant';

describe('SmbClusterResourcePageComponent', () => {
  let component: SmbClusterResourcePageComponent;
  let fixture: ComponentFixture<SmbClusterResourcePageComponent>;

  // Use a BehaviorSubject to mock the stream so we can emit different values in tests
  let clusterSubject: BehaviorSubject<SMBCluster | null>;
  let mockSmbClusterResourceStateService: { cluster$: any };

  const setup = async (ctx: SmbContext = SMB_CEPHFS_CONTEXT) => {
    // Default emission for a successful load
    clusterSubject = new BehaviorSubject<SMBCluster | null>({
      cluster_id: 'test-cluster',
      auth_mode: 'active_directory'
    } as SMBCluster);

    mockSmbClusterResourceStateService = {
      cluster$: clusterSubject.asObservable()
    };

    await TestBed.configureTestingModule({
      declarations: [SmbClusterResourcePageComponent],
      providers: [
        { provide: SMB_CONTEXT, useValue: ctx },
        {
          provide: SmbClusterResourceStateService,
          useValue: mockSmbClusterResourceStateService
        },
        {
          provide: ActivatedRoute,
          useValue: {
            data: of({ section: 'overview' }),
            parent: {
              paramMap: of(convertToParamMap({ cluster_id: 'test-cluster' }))
            }
          }
        }
      ],
      // Ignores unknown child components in the template
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbClusterResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  };

  beforeEach(async () => {
    await setup();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should read section from route data', () => {
    expect(component.section).toBe('overview');
  });

  it('should build overviewFields with correct labels and values after init', () => {
    expect(component.overviewFields.length).toBe(3);
    expect(component.overviewFields[0]).toEqual({
      label: 'Name',
      value: 'test-cluster'
    });
    expect(component.overviewFields[1]).toEqual({
      label: 'Authentication Mode',
      value: 'active_directory'
    });
    expect(component.overviewFields[2]).toEqual({
      label: 'Compatibility',
      value: CLIENT_COMPAT.Default
    });
  });

  it('should show macos compatibility when client_compat is set', () => {
    clusterSubject.next({
      cluster_id: 'test-cluster',
      auth_mode: 'active_directory',
      client_compat: CLIENT_COMPAT.MacOS
    } as SMBCluster);
    fixture.detectChanges();

    expect(component.overviewFields[2]).toEqual({
      label: 'Compatibility',
      value: CLIENT_COMPAT.MacOS
    });
  });

  it('should not show compatibility field for rgw smb', async () => {
    TestBed.resetTestingModule();
    await setup(SMB_RGW_CONTEXT);

    expect(component.overviewFields.length).toBe(2);
    expect(component.overviewFields.map((f) => f.label)).not.toContain('Compatibility');
  });

  it('should set loadError = true and selection = undefined when cluster emits null (service error)', () => {
    // Emit null to simulate no cluster being found / an error occurring
    clusterSubject.next(null);
    fixture.detectChanges();

    expect(component.selection).toBeUndefined();
    expect(component.loadError).toBe(true);
    expect(component.overviewFields.length).toBe(0);
  });
});
