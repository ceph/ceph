import { Component, Input, NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { BehaviorSubject, of } from 'rxjs';

@Component({
  selector: 'cd-nfs-list',
  template: '',
  standalone: false
})
class NfsListStubComponent {
  static instances = 0;

  @Input()
  clusterId: string;

  constructor() {
    NfsListStubComponent.instances += 1;
  }
}

import { PlacementPipe } from '~/app/ceph/cluster/services/placement.pipe';
import { NfsClusterResourceStateService } from '~/app/shared/services/nfs-cluster-resource-state.service';
import { NFSCluster } from '../models/nfs-cluster-config';
import { NfsClusterResourcePageComponent } from './nfs-cluster-resource-page.component';

describe('NfsClusterResourcePageComponent', () => {
  let component: NfsClusterResourcePageComponent;
  let fixture: ComponentFixture<NfsClusterResourcePageComponent>;
  let clusterSubject: BehaviorSubject<NFSCluster | null>;

  const activeCluster: NFSCluster = {
    name: 'demo-nfs-cluster',
    virtual_ip: null,
    port: 2049,
    monitor_port: 9587,
    deployment_type: 'standalone',
    backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }],
    placement: { count: 10 }
  };

  beforeEach(async () => {
    clusterSubject = new BehaviorSubject<NFSCluster | null>(activeCluster);
    NfsListStubComponent.instances = 0;

    await TestBed.configureTestingModule({
      declarations: [NfsClusterResourcePageComponent, NfsListStubComponent],
      providers: [
        PlacementPipe,
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
    expect(component.overviewFields.find((f) => f.label === 'Placement')?.value).toBe('count:10');
    expect(component.overviewFields.find((f) => f.label === 'Service instances')?.value).toBe(
      '1 / 10'
    );
    const haField = component.overviewFields.find((f) => f.label === 'High availability');
    expect(haField?.value).toBe('Disabled');
    expect(haField?.type).toBe('status');
    expect(haField?.status).toBe('danger');
  });

  it('should format hosts and label placement like the cluster table', () => {
    clusterSubject.next({
      ...activeCluster,
      placement: { count: 2, hosts: ['host1', 'host2'] }
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Placement')?.value).toBe(
      'host1;host2;count:2'
    );

    clusterSubject.next({
      ...activeCluster,
      placement: { label: 'nfs' }
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Placement')?.value).toBe('label:nfs');
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
      virtual_ip: null,
      deployment_type: 'standalone',
      backend: [{ hostname: 'host1', ip: '1.2.3.4', port: 12049, status: 'running' }]
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Service port')?.value).toBe(12049);
  });

  it('should show RDMA port only when RDMA is enabled', () => {
    expect(component.overviewFields.find((f) => f.label === 'RDMA port')).toBeUndefined();

    clusterSubject.next({
      ...activeCluster,
      enable_rdma: true,
      rdma_port: 20049
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'RDMA port')?.value).toBe(20049);
  });

  it('should show dash when no service port is available', () => {
    clusterSubject.next({
      name: 'standalone-nfs',
      virtual_ip: null,
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
    expect(component.overviewFields.find((f) => f.label === 'Placement')?.value).toBe(
      'host_pattern:nfs-*'
    );
  });

  it('should show host_pattern placement from pattern object', () => {
    clusterSubject.next({
      ...activeCluster,
      placement: { host_pattern: { pattern: 'nfs-.*', pattern_type: 'regex' } }
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Placement')?.value).toBe(
      'host_pattern:nfs-.*'
    );
  });

  it('should recreate the shares list when the cluster name changes', () => {
    expect(NfsListStubComponent.instances).toBe(1);

    clusterSubject.next({
      ...activeCluster,
      port: 2050
    });
    fixture.detectChanges();
    expect(NfsListStubComponent.instances).toBe(1);

    clusterSubject.next({
      ...activeCluster,
      name: 'other-cluster'
    });
    fixture.detectChanges();
    expect(NfsListStubComponent.instances).toBe(2);
    expect(fixture.nativeElement.querySelector('cd-nfs-list')).toBeTruthy();
  });

  it('should show a fetch error instead of default cluster settings', () => {
    clusterSubject.next({
      name: 'demo-nfs-cluster',
      virtual_ip: null,
      backend: [],
      placement: {},
      error: 'failed to describe service'
    });
    fixture.detectChanges();

    expect(component.loadError).toBe(false);
    expect(component.selection?.error).toBe('failed to describe service');
    expect(component.overviewFields).toEqual([]);
    expect(fixture.nativeElement.querySelector('cd-resource-overview-card')).toBeFalsy();
    expect(fixture.nativeElement.textContent).toContain('failed to describe service');
  });

  it('should set loadError when cluster is missing', () => {
    clusterSubject.next(null);
    fixture.detectChanges();
    expect(component.loadError).toBe(true);
    expect(component.selection).toBeUndefined();
    expect(component.overviewFields.length).toBe(0);
  });
});
