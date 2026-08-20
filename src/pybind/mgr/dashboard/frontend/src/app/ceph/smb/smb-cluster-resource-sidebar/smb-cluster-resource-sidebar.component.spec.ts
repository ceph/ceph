import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { BehaviorSubject, of } from 'rxjs';

import { SmbClusterResourceSidebarComponent } from './smb-cluster-resource-sidebar.component';
import { SmbClusterResourceStateService } from '~/app/shared/services/smb-cluster-resource-state.service';
import { SMBCluster } from '../smb.model';

describe('SmbClusterResourceSidebarComponent', () => {
  let component: SmbClusterResourceSidebarComponent;
  let fixture: ComponentFixture<SmbClusterResourceSidebarComponent>;
  let clusterSubject: BehaviorSubject<SMBCluster | null>;
  let mockStateService: { cluster$: any; load: jest.Mock };

  beforeEach(async () => {
    // Setup a BehaviorSubject so we can control what the cluster$ observable emits
    clusterSubject = new BehaviorSubject<SMBCluster | null>({
      cluster_id: 'test-cluster',
      auth_mode: 'active_directory'
    } as SMBCluster);

    // Mock the state service
    mockStateService = {
      cluster$: clusterSubject.asObservable(),
      load: jest.fn()
    };

    await TestBed.configureTestingModule({
      declarations: [SmbClusterResourceSidebarComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ cluster_id: 'test-cluster' }))
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    })
      // Crucial: Override the component's internal provider to use our mock
      .overrideComponent(SmbClusterResourceSidebarComponent, {
        set: {
          providers: [{ provide: SmbClusterResourceStateService, useValue: mockStateService }]
        }
      })
      .compileComponents();

    fixture = TestBed.createComponent(SmbClusterResourceSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set clusterId from route, call load on state service, and build sidebar', () => {
    // Verify that the route param was read correctly
    expect(component.clusterId).toBe('test-cluster');

    // Verify that the component called load() on the state service
    expect(mockStateService.load).toHaveBeenCalledWith('test-cluster');

    // Verify that the clusterName and selection were set correctly from the observable emission
    expect(component.clusterName).toBe('test-cluster');
    expect(component.selection).toEqual({
      cluster_id: 'test-cluster',
      auth_mode: 'active_directory'
    });

    // Verify that the sidebar items were built
    expect(component.sidebarItems.length).toBe(1);
    expect(component.sidebarItems[0].route).toEqual([
      '/cephfs/smb/cluster',
      'test-cluster',
      'overview'
    ]);
  });

  it('should fall back to clusterId for clusterName if cluster$ emits null', () => {
    // Emit null to simulate an initial loading state or a fetch error
    clusterSubject.next(null);
    fixture.detectChanges();
    expect(component.selection).toBeUndefined();
    // It should fall back to the raw route param ID
    expect(component.clusterName).toBe('test-cluster');
  });
});
