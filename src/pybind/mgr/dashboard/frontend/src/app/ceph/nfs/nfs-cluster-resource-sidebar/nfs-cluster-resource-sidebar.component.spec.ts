import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap, Router } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { BehaviorSubject, of } from 'rxjs';

import { NfsClusterResourceSidebarComponent } from './nfs-cluster-resource-sidebar.component';
import { NfsClusterResourceStateService } from '~/app/shared/services/nfs-cluster-resource-state.service';
import { NFSCluster } from '../models/nfs-cluster-config';

describe('NfsClusterResourceSidebarComponent', () => {
  let component: NfsClusterResourceSidebarComponent;
  let fixture: ComponentFixture<NfsClusterResourceSidebarComponent>;
  let clusterSubject: BehaviorSubject<NFSCluster | null>;
  let mockStateService: { cluster$: any; load: jest.Mock };

  const configure = async (url: string) => {
    clusterSubject = new BehaviorSubject<NFSCluster | null>({
      name: 'demo-nfs-cluster',
      backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }]
    });

    mockStateService = {
      cluster$: clusterSubject.asObservable(),
      load: jest.fn()
    };

    await TestBed.configureTestingModule({
      declarations: [NfsClusterResourceSidebarComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ cluster_id: 'demo-nfs-cluster' }))
          }
        },
        {
          provide: Router,
          useValue: { url }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .overrideComponent(NfsClusterResourceSidebarComponent, {
        set: {
          providers: [{ provide: NfsClusterResourceStateService, useValue: mockStateService }]
        }
      })
      .compileComponents();

    fixture = TestBed.createComponent(NfsClusterResourceSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  };

  beforeEach(async () => {
    await configure('/cephfs/nfs/cluster/demo-nfs-cluster/overview');
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set clusterId from route, call load, and build sidebar', () => {
    expect(component.clusterId).toBe('demo-nfs-cluster');
    expect(mockStateService.load).toHaveBeenCalledWith('demo-nfs-cluster');
    expect(component.clusterName).toBe('demo-nfs-cluster');
    expect(component.selection?.name).toBe('demo-nfs-cluster');
    expect(component.sidebarItems.length).toBe(1);
    expect(component.sidebarItems[0].route).toEqual([
      '/cephfs/nfs/cluster',
      'demo-nfs-cluster',
      'overview'
    ]);
  });

  it('should build rgw sidebar routes under Object NFS', async () => {
    TestBed.resetTestingModule();
    await configure('/rgw/nfs/cluster/demo-nfs-cluster/overview');
    expect(component.sidebarItems[0].route).toEqual([
      '/rgw/nfs/cluster',
      'demo-nfs-cluster',
      'overview'
    ]);
  });

  it('should fall back to clusterId for clusterName when cluster$ emits null', () => {
    clusterSubject.next(null);
    fixture.detectChanges();
    expect(component.selection).toBeUndefined();
    expect(component.clusterName).toBe('demo-nfs-cluster');
  });
});
