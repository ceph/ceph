import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of, throwError } from 'rxjs';
import { take } from 'rxjs/operators';

import { NfsService } from '~/app/shared/api/nfs.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';

import { NFSCluster, NFSShare } from '../models/nfs.model';
import { NfsClusterComponent } from './nfs-cluster.component';

describe('NfsClusterComponent', () => {
  let component: NfsClusterComponent;
  let fixture: ComponentFixture<NfsClusterComponent>;
  let nfsService: NfsService;
  let listClustersSpy: jasmine.Spy;
  let listSpy: jasmine.Spy;

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
    listClustersSpy = spyOn(nfsService, 'listClusters').and.returnValue(
      of([
        {
          name: 'cluster1',
          backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }],
          enable_nfsv3: true
        }
      ])
    );
    listSpy = spyOn(nfsService, 'list').and.returnValue(
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

  describe('columns', () => {
    it('should configure columns for Name, Status, Shares and Protocol', () => {
      expect(component.columns.map((column) => column.name)).toEqual([
        'Name',
        'Status',
        'Shares',
        'Protocol'
      ]);
    });

    it('should bind columns to the expected row properties', () => {
      expect(component.columns.map((column) => column.prop)).toEqual([
        'name',
        'status',
        'shares',
        'protocol'
      ]);
    });

    it('should render Status with the status class mapping', () => {
      const statusColumn = component.columns.find((column) => column.prop === 'status');
      expect(statusColumn.cellTemplate).toBeTruthy();
      expect(statusColumn.customTemplateConfig.map).toEqual({
        Running: { class: 'tag-success' },
        Degraded: { class: 'tag-warning' },
        Stopped: { class: 'tag-danger' },
        Unknown: { class: 'tag-default' }
      });
    });
  });

  describe('status and row mapping', () => {
    async function expectClusterRows(
      clusters: NFSCluster[],
      exports: NFSShare[],
      expected: object[]
    ) {
      listClustersSpy.and.returnValue(of(clusters));
      listSpy.and.returnValue(of(exports));
      // Re-subscribe so BehaviorSubject emits with the updated spies.
      const rows = await component.clusters$.pipe(take(1)).toPromise();
      expect(rows).toEqual(expected.map((row) => jasmine.objectContaining(row)));
    }

    it('should map cluster rows with status, shares and protocol', async () => {
      await expectClusterRows(
        [
          {
            name: 'cluster1',
            backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }],
            enable_nfsv3: true
          } as NFSCluster
        ],
        [
          { cluster_id: 'cluster1', export_id: 1 },
          { cluster_id: 'cluster1', export_id: 2 }
        ],
        [
          {
            name: 'cluster1',
            status: 'Running',
            shares: 2,
            protocol: 'NFSv3, NFSv4'
          }
        ]
      );
    });

    it('should mark status Running when all backends are running', async () => {
      await expectClusterRows(
        [
          {
            name: 'cluster1',
            backend: [
              { hostname: 'host1', ip: '1.2.3.4', status: 'running' },
              { hostname: 'host2', ip: '1.2.3.5', status: 'RUNNING' }
            ]
          } as NFSCluster
        ],
        [],
        [{ name: 'cluster1', status: 'Running' }]
      );
    });

    it('should mark status Degraded when some backends are running', async () => {
      await expectClusterRows(
        [
          {
            name: 'cluster1',
            backend: [
              { hostname: 'host1', ip: '1.2.3.4', status: 'running' },
              { hostname: 'host2', ip: '1.2.3.5', status: 'stopped' }
            ]
          } as NFSCluster
        ],
        [],
        [{ name: 'cluster1', status: 'Degraded' }]
      );
    });

    it('should show a dash when no backends are running', async () => {
      await expectClusterRows(
        [
          {
            name: 'cluster1',
            backend: [
              { hostname: 'host1', ip: '1.2.3.4', status: 'stopped' },
              { hostname: 'host2', ip: '1.2.3.5', status: 'error' }
            ]
          } as NFSCluster
        ],
        [],
        [{ name: 'cluster1', status: '-' }]
      );
    });

    it('should show a dash when backends are missing', async () => {
      await expectClusterRows(
        [
          {
            name: 'cluster1',
            backend: []
          } as NFSCluster,
          {
            name: 'cluster2'
          } as NFSCluster
        ],
        [],
        [
          { name: 'cluster1', status: '-', eventsLink: ['/services', 'nfs.cluster1', 'events'] },
          { name: 'cluster2', status: '-', eventsLink: ['/services', 'nfs.cluster2', 'events'] }
        ]
      );
    });

    it('should set protocol to NFSv4 when nfsv3 is disabled', async () => {
      await expectClusterRows(
        [
          {
            name: 'cluster1',
            backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }],
            enable_nfsv3: false
          } as NFSCluster
        ],
        [],
        [{ name: 'cluster1', protocol: 'NFSv4' }]
      );
    });

    it('should count shares per cluster and default to zero', async () => {
      await expectClusterRows(
        [
          {
            name: 'cluster1',
            backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }]
          } as NFSCluster,
          {
            name: 'cluster2',
            backend: [{ hostname: 'host2', ip: '1.2.3.5', status: 'running' }]
          } as NFSCluster
        ],
        [
          { cluster_id: 'cluster1', export_id: 1 },
          { cluster_id: 'cluster1', export_id: 2 },
          { cluster_id: 'cluster3', export_id: 3 },
          { export_id: 4 } as NFSShare
        ],
        [
          { name: 'cluster1', shares: 2 },
          { name: 'cluster2', shares: 0 }
        ]
      );
    });

    it('should return empty rows when listing clusters fails', async () => {
      listClustersSpy.and.returnValue(throwError(new Error('failed')));
      const clusters = await component.clusters$.pipe(take(1)).toPromise();
      expect(clusters).toEqual([]);
    });

    it('should still map clusters when listing exports fails', async () => {
      listSpy.and.returnValue(throwError(new Error('failed')));
      const clusters = await component.clusters$.pipe(take(1)).toPromise();
      expect(clusters).toEqual([
        jasmine.objectContaining({
          name: 'cluster1',
          status: 'Running',
          shares: 0,
          protocol: 'NFSv3, NFSv4'
        })
      ]);
    });
  });
});
