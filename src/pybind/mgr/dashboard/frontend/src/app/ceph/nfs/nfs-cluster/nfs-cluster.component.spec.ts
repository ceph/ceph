import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { NfsService } from '~/app/shared/api/nfs.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';

import { NfsClusterComponent } from './nfs-cluster.component';

describe('NfsClusterComponent', () => {
  let component: NfsClusterComponent;
  let fixture: ComponentFixture<NfsClusterComponent>;
  let nfsService: NfsService;

  configureTestBed({
    declarations: [NfsClusterComponent],
    imports: [HttpClientTestingModule, SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsClusterComponent);
    component = fixture.componentInstance;
    nfsService = TestBed.inject(NfsService);
    const orchService = TestBed.inject(OrchestratorService);
    spyOn(orchService, 'status').and.returnValue(of({ available: true, message: '' }));
    spyOn(nfsService, 'nfsClusterList').and.returnValue(
      of([
        {
          name: 'cluster1',
          backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }],
          enable_nfsv3: true
        }
      ])
    );
    spyOn(nfsService, 'list').and.returnValue(
      of([
        { cluster_id: 'cluster1', export_id: 1 },
        { cluster_id: 'cluster1', export_id: 2 }
      ])
    );
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should configure columns for Name, Status, Shares and Protocol', () => {
    expect(component.columns.map((column) => column.name)).toEqual([
      'Name',
      'Status',
      'Shares',
      'Protocol'
    ]);
  });

  it('should map cluster rows with status, shares and protocol', (done) => {
    component.clusters$.subscribe((clusters) => {
      expect(clusters).toEqual([
        jasmine.objectContaining({
          name: 'cluster1',
          status: 'Running',
          shares: 2,
          protocol: 'NFSv3, NFSv4'
        })
      ]);
      done();
    });
    component.loadData();
  });
});
