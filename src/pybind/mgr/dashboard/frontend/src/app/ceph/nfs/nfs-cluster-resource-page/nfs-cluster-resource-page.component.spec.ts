import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { BehaviorSubject, of } from 'rxjs';

import { NfsClusterResourcePageComponent } from './nfs-cluster-resource-page.component';
import { NfsClusterResourceStateService } from '~/app/shared/services/nfs-cluster-resource-state.service';
import { NFSCluster } from '../models/nfs-cluster-config';

describe('NfsClusterResourcePageComponent', () => {
  let component: NfsClusterResourcePageComponent;
  let fixture: ComponentFixture<NfsClusterResourcePageComponent>;
  let clusterSubject: BehaviorSubject<NFSCluster | null>;

  const activeCluster: NFSCluster = {
    name: 'demo-nfs-cluster',
    port: 2049,
    monitor_port: 9587,
    deployment_type: 'standalone',
    backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }],
    placement: { count: 10 }
  };

  beforeEach(async () => {
    clusterSubject = new BehaviorSubject<NFSCluster | null>(activeCluster);

    await TestBed.configureTestingModule({
      declarations: [NfsClusterResourcePageComponent],
      providers: [
        {
          provide: NfsClusterResourceStateService,
          useValue: { cluster$: clusterSubject.asObservable() }
        },
        {
          provide: ActivatedRoute,
          useValue: {
            data: of({ section: 'overview' }),
            parent: {
              paramMap: of(convertToParamMap({ cluster_id: 'demo-nfs-cluster' }))
            }
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(NfsClusterResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should read section from route data', () => {
    expect(component.section).toBe('overview');
  });

  it('should build overview fields after init', () => {
    expect(component.overviewFields.find((f) => f.label === 'Name')?.value).toBe(
      'demo-nfs-cluster'
    );
    expect(component.overviewFields.find((f) => f.label === 'Status')?.value).toBe('Active');
    expect(component.overviewFields.find((f) => f.label === 'Service port')?.value).toBe(2049);
    expect(component.overviewFields.find((f) => f.label === 'Protocol')?.value).toBe('NFSv4');
  });

  it('should show NFSv3 and NFSv4 when enable_nfsv3 is set', () => {
    clusterSubject.next({
      ...activeCluster,
      enable_nfsv3: true
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Protocol')?.value).toBe(
      'NFSv3, NFSv4'
    );
  });

  it('should use backend port when top-level port is missing', () => {
    clusterSubject.next({
      name: 'standalone-nfs',
      deployment_type: 'standalone',
      backend: [{ hostname: 'host1', ip: '1.2.3.4', port: 12049, status: 'running' }]
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Service port')?.value).toBe(12049);
  });

  it('should show dash when no service port is available', () => {
    clusterSubject.next({
      name: 'standalone-nfs',
      deployment_type: 'standalone',
      backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }]
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Service port')?.value).toBe('-');
  });

  it('should show host_pattern placement as a string', () => {
    clusterSubject.next({
      ...activeCluster,
      placement: { host_pattern: 'nfs-*' }
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Placement')?.value).toBe('nfs-*');
  });

  it('should show host_pattern placement from pattern object', () => {
    clusterSubject.next({
      ...activeCluster,
      placement: { host_pattern: { pattern: 'nfs-.*', pattern_type: 'regex' } }
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Placement')?.value).toBe('nfs-.*');
  });

  it('should set loadError when cluster is missing', () => {
    clusterSubject.next(null);
    fixture.detectChanges();
    expect(component.loadError).toBe(true);
    expect(component.selection).toBeUndefined();
    expect(component.overviewFields.length).toBe(0);
  });
});
