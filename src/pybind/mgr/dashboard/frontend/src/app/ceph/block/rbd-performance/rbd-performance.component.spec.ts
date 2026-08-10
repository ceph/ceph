import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RbdTabsComponent } from '../rbd-tabs/rbd-tabs.component';
import { RbdPerformanceComponent } from './rbd-performance.component';
import { RbdService } from '~/app/shared/api/rbd.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { PerformanceCardComponent } from '~/app/shared/components/performance-card/performance-card.component';

describe('RbdPerformanceComponent', () => {
  let component: RbdPerformanceComponent;
  let fixture: ComponentFixture<RbdPerformanceComponent>;
  let rbdService: RbdService;
  let poolService: PoolService;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      NgbNavModule,
      PerformanceCardComponent
    ],
    declarations: [RbdPerformanceComponent, RbdTabsComponent]
  });

  beforeEach(() => {
    rbdService = TestBed.inject(RbdService);
    poolService = TestBed.inject(PoolService);

    spyOn(rbdService, 'list').and.returnValue(
      of([
        { pool_name: 'pool1', value: [{ name: 'img1' }, { name: 'img2' }] },
        { pool_name: 'pool2', value: [{ name: 'img3' }] }
      ])
    );
    spyOn(rbdService, 'listTrash').and.returnValue(of([{ name: 'trash1' }]));
    spyOn(poolService, 'list').and.returnValue(
      Promise.resolve([
        { pool_name: 'pool1', type: 'replicated', application_metadata: ['rbd'] },
        { pool_name: 'pool2', type: 'replicated', application_metadata: ['rbd'] }
      ])
    );
    spyOn(rbdService, 'isRBDPool').and.callFake(() => true);
    spyOn(rbdService, 'listNamespaces').and.callFake((poolName: string) => {
      if (poolName === 'pool1') {
        return of([{ namespace: 'ns1' }]);
      }
      return of([]);
    });

    fixture = TestBed.createComponent(RbdPerformanceComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should calculate counts correctly', fakeAsync(() => {
    fixture.detectChanges();
    tick(); // resolve the pool list promise
    fixture.detectChanges();
    tick(); // resolve listNamespaces forkJoin observables

    expect(component.totalPools).toBe(2);
    expect(component.totalImages).toBe(3);
    expect(component.totalTrashImages).toBe(1);
    expect(component.totalNamespaces).toBe(1);
    expect(component.loading).toBeFalsy();
  }));
});
