import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { of, throwError } from 'rxjs';
import { take } from 'rxjs/operators';

import { NfsService } from '~/app/shared/api/nfs.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
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
    imports: [HttpClientTestingModule, RouterTestingModule, SharedModule]
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

  it('should refresh the cluster table every 5 seconds', () => {
    const table = fixture.debugElement.query(By.directive(TableComponent));
    expect(table.componentInstance.autoReload).toBe(5000);
  });

  describe('columns', () => {
    it('should configure columns for Name, Status, Placement, Ingress mode, Shares and Protocol', () => {
      expect(component.columns.map((column) => column.name)).toEqual([
        'Name',
        'Status',
        'Placement',
        'Ingress mode',
        'Shares',
        'Protocol'
      ]);
    });

    it('should bind columns to the expected row properties', () => {
      expect(component.columns.map((column) => column.prop)).toEqual([
        'name',
        'statuses',
        'placementDisplay',
        'ingressModeDisplay',
        'shares',
        'protocol'
      ]);
    });

    it('should render Status with the shared tag mapping', () => {
      const statusColumn = component.columns.find((column) => column.prop === 'statuses');
      expect(statusColumn.cellTransformation).toBe(CellTemplate.tag);
      expect(statusColumn.customTemplateConfig.class).toBe('tag-dark');
      expect(statusColumn.customTemplateConfig.map).toEqual({
        running: { class: 'tag-success' },
        starting: { class: 'tag-dark' },
        stopped: { class: 'tag-warning' },
        error: { class: 'tag-danger' },
        unknown: { class: 'tag-dark' }
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

    it('should map cluster rows with status, placement, ingress mode, shares and protocol', async () => {
      await expectClusterRows(
        [
          {
            name: 'cluster1',
            backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }],
            enable_nfsv3: true,
            placement: { count: 2, hosts: ['host1', 'host2'] },
            ingress: true,
            ingress_mode: 'haproxy-protocol'
          } as NFSCluster
        ],
        [
          { cluster_id: 'cluster1', export_id: 1 },
          { cluster_id: 'cluster1', export_id: 2 }
        ],
        [
          {
            name: 'cluster1',
            statuses: ['running'],
            shares: 2,
            protocol: 'NFSv3, NFSv4',
            placementDisplay: 'host1;host2;count:2',
            ingressModeDisplay: 'HAProxy protocol'
          }
        ]
      );
    });

    it('should show a dash for empty placement and disabled ingress', async () => {
      await expectClusterRows(
        [
          {
            name: 'cluster1',
            backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }],
            placement: {},
            ingress: false
          } as NFSCluster
        ],
        [],
        [
          {
            name: 'cluster1',
            placementDisplay: '-',
            ingressModeDisplay: '-'
          }
        ]
      );
    });

    it('should format ingress mode labels', async () => {
      await expectClusterRows(
        [
          {
            name: 'cluster1',
            backend: [],
            placement: { label: 'nfs' },
            ingress: true,
            ingress_mode: 'keepalive-only'
          } as NFSCluster
        ],
        [],
        [
          {
            name: 'cluster1',
            placementDisplay: 'label:nfs',
            ingressModeDisplay: 'Keepalive only'
          }
        ]
      );
    });

    it('should show each backend status', async () => {
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
        [
          {
            name: 'cluster1',
            statuses: ['running', 'running']
          }
        ]
      );
    });

    it('should show stopped and error statuses directly', async () => {
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
        [
          {
            name: 'cluster1',
            statuses: ['running', 'stopped']
          }
        ]
      );
    });

    it('should show stopped and error instead of a dash', async () => {
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
        [
          {
            name: 'cluster1',
            statuses: ['stopped', 'error']
          }
        ]
      );
    });

    it('should show unknown when backends are missing', async () => {
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
          {
            name: 'cluster1',
            statuses: ['unknown']
          },
          {
            name: 'cluster2',
            statuses: ['unknown']
          }
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
          statuses: ['running'],
          shares: 0,
          protocol: 'NFSv3, NFSv4'
        })
      ]);
    });
  });

  it('should link cluster name to the cephfs cluster overview page', () => {
    Object.defineProperty(TestBed.inject(Router), 'url', {
      get: () => '/cephfs/nfs/cluster'
    });
    component.ngOnInit();
    expect(component.columns[0].prop).toBe('name');
    expect(component.columns[0].cellTransformation).toBe(CellTemplate.redirect);
    expect(component.columns[0].customTemplateConfig).toEqual({
      redirectLink: ['/cephfs/nfs/cluster', '::prop', 'overview']
    });
  });

  it('should link cluster name to the rgw cluster overview page', () => {
    Object.defineProperty(TestBed.inject(Router), 'url', {
      get: () => '/rgw/nfs/cluster'
    });
    component.ngOnInit();
    expect(component.columns[0].customTemplateConfig).toEqual({
      redirectLink: ['/rgw/nfs/cluster', '::prop', 'overview']
    });
  });

  it('should include edit table action alongside create and delete', () => {
    expect(component.tableActions.map((action) => action.name)).toEqual([
      component.actionLabels.CREATE,
      component.actionLabels.EDIT,
      component.actionLabels.DELETE
    ]);
    const edit = component.tableActions.find((a) => a.name === component.actionLabels.EDIT);
    expect(edit.permission).toBe('update');
    expect(edit.disable({ hasSingleSelection: false } as any)).toBe(true);
    expect(edit.disable({ hasSingleSelection: true } as any)).toBe(false);
  });
});
