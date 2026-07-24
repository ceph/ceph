import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';
import { GridModule, TilesModule } from 'carbon-components-angular';

import { RbdOverviewComponent } from './rbd-overview.component';
import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RbdOverviewComponent', () => {
  let component: RbdOverviewComponent;
  let fixture: ComponentFixture<RbdOverviewComponent>;
  let rbdMirroringService: RbdMirroringService;
  let rbdService: RbdService;
  let poolService: PoolService;

  configureTestBed({
    declarations: [RbdOverviewComponent],
    imports: [
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      GridModule,
      TilesModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdOverviewComponent);
    component = fixture.componentInstance;

    rbdMirroringService = TestBed.inject(RbdMirroringService);
    rbdService = TestBed.inject(RbdService);
    poolService = TestBed.inject(PoolService);

    spyOn(rbdMirroringService, 'startPolling').and.returnValue(of({}).subscribe());
    spyOn(rbdMirroringService, 'subscribeSummary').and.callFake((fn: any) => {
      fn({
        site_name: 'site-alpha',
        content_data: {
          daemons: [{ id: 'daemon1' }],
          errors: 0,
          warnings: 0,
          pools: [{ name: 'pool1', peer_uuids: ['peer-1'] }]
        }
      });
      return of({}).subscribe();
    });

    spyOn(poolService, 'getList').and.returnValue(
      of([
        {
          pool_name: 'rbd_pool',
          application_metadata: ['rbd'],
          stats: {
            bytes_used: { latest: 1073741824 },
            max_avail: { latest: 10737418240 }
          }
        }
      ])
    );

    spyOn(rbdService, 'list').and.returnValue(
      of([
        {
          pool_name: 'rbd_pool',
          value: [
            { name: 'img2', size: 21474836480, num_objs: 512, format: 2, snapshots: [], disk_usage: 1073741824 },
            { name: 'img1', size: 10737418240, num_objs: 256, format: 2, snapshots: [{ id: 1 }], disk_usage: 0 }
          ]
        }
      ])
    );

    spyOn(rbdService, 'listNamespaces').and.returnValue(
      of([{ namespace: 'namespace1' }])
    );

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load mirroring status and health correctly', () => {
    expect(component.daemonCount).toBe(1);
    expect(component.healthStatus).toBe('OK');
    expect(component.totalPeers).toBe(1);
  });

  it('should calculate capacity and counts correctly', () => {
    expect(component.totalPools).toBe(1);
    expect(component.rawUsedBytes).toBe(1073741824);
    expect(component.totalImages).toBe(2);
    expect(component.provisionedBytes).toBe(32212254720);
    expect(component.totalSnapshots).toBe(1);
  });

  it('should sort top images by provisioned size descending', () => {
    expect(component.topImages.length).toBe(2);
    expect(component.topImages[0].name).toBe('img2');
    expect(component.topImages[1].name).toBe('img1');
  });
});
