import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Component, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { TreeModule } from 'angular-tree-component';
import * as _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { CephfsService } from '../../../shared/api/cephfs.service';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { SharedModule } from '../../../shared/shared.module';
import { CephfsClientsComponent } from '../cephfs-clients/cephfs-clients.component';
import { CephfsDetailComponent } from '../cephfs-detail/cephfs-detail.component';
import { CephfsDirectoriesComponent } from '../cephfs-directories/cephfs-directories.component';
import { CephfsTabsComponent } from './cephfs-tabs.component';

describe('CephfsTabsComponent', () => {
  let component: CephfsTabsComponent;
  let fixture: ComponentFixture<CephfsTabsComponent>;
  let service: CephfsService;
  let data: {
    standbys: string;
    pools: any[];
    ranks: any[];
    mdsCounters: object;
    name: string;
    clients: { status: ViewCacheStatus; data: any[] };
  };

  let old: any;
  const getReload: any = () => component['reloadSubscriber'];
  const setReload = (sth?: any) => (component['reloadSubscriber'] = sth);
  const mockRunOutside = () => {
    component['subscribeInterval'] = () => {
      // It's mocked because the rxjs timer subscription ins't called through the use of 'tick'.
      setReload({
        unsubscribed: false,
        unsubscribe: () => {
          old = getReload();
          getReload().unsubscribed = true;
          setReload();
        }
      });
      component.refresh();
    };
  };

  const setSelection = (selection: any) => {
    component.selection = selection;
    component.ngOnChanges();
  };

  const selectFs = (id: number, name: string) => {
    setSelection({
      id,
      mdsmap: {
        info: {
          something: {
            name
          }
        }
      }
    });
  };

  const updateData = () => {
    component['data'] = _.cloneDeep(data);
    component.softRefresh();
  };

  @Component({ selector: 'cd-cephfs-chart', template: '' })
  class CephfsChartStubComponent {
    @Input()
    mdsCounter: any;
  }

  configureTestBed({
    imports: [
      SharedModule,
      NgbNavModule,
      HttpClientTestingModule,
      TreeModule,
      ToastrModule.forRoot()
    ],
    declarations: [
      CephfsTabsComponent,
      CephfsChartStubComponent,
      CephfsDetailComponent,
      CephfsDirectoriesComponent,
      CephfsClientsComponent
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsTabsComponent);
    component = fixture.componentInstance;
    component.selection = undefined;
    data = {
      standbys: 'b',
      pools: [{}, {}],
      ranks: [{}, {}, {}],
      mdsCounters: { a: { name: 'a', x: [], y: [] } },
      name: 'someFs',
      clients: {
        status: ViewCacheStatus.ValueOk,
        data: [{}, {}, {}, {}]
      }
    };
    service = TestBed.inject(CephfsService);
    spyOn(service, 'getTabs').and.callFake(() => of(data));

    fixture.detectChanges();
    mockRunOutside();
    setReload(); // Clears rxjs timer subscription
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should resist invalid mds info', () => {
    setSelection({
      id: 3,
      mdsmap: {
        info: {}
      }
    });
    expect(component.grafanaId).toBe(undefined);
  });

  it('should find out the grafana id', () => {
    selectFs(2, 'otherMds');
    expect(component.grafanaId).toBe('otherMds');
  });

  it('should set default values on id change before api request', () => {
    const defaultDetails: Record<string, any> = {
      standbys: '',
      pools: [],
      ranks: [],
      mdsCounters: {},
      name: ''
    };
    const defaultClients: Record<string, any> = {
      data: [],
      status: ViewCacheStatus.ValueNone
    };
    component['subscribeInterval'] = () => undefined;
    updateData();
    expect(component.clients).not.toEqual(defaultClients);
    expect(component.details).not.toEqual(defaultDetails);
    selectFs(2, 'otherMds');
    expect(component.clients).toEqual(defaultClients);
    expect(component.details).toEqual(defaultDetails);
  });

  it('should force data updates on tab change without api requests', () => {
    const oldClients = component.clients;
    const oldDetails = component.details;
    updateData();
    expect(service.getTabs).toHaveBeenCalledTimes(0);
    expect(component.details).not.toBe(oldDetails);
    expect(component.clients).not.toBe(oldClients);
  });

  describe('handling of id change', () => {
    beforeEach(() => {
      setReload(); // Clears rxjs timer subscription
      selectFs(2, 'otherMds');
      old = getReload(); // Gets current subscription
    });

    it('should have called getDetails once', () => {
      expect(component.details.pools.length).toBe(2);
      expect(service.getTabs).toHaveBeenCalledTimes(1);
    });

    it('should not subscribe to an new interval for the same selection', () => {
      expect(component.id).toBe(2);
      expect(component.grafanaId).toBe('otherMds');
      selectFs(2, 'otherMds');
      expect(component.id).toBe(2);
      expect(component.grafanaId).toBe('otherMds');
      expect(getReload()).toBe(old);
    });

    it('should subscribe to an new interval', () => {
      selectFs(3, 'anotherMds');
      expect(getReload()).not.toBe(old); // Holds an new object
    });

    it('should unsubscribe the old interval if it exists', () => {
      selectFs(3, 'anotherMds');
      expect(old.unsubscribed).toBe(true);
    });

    it('should not unsubscribe if no interval exists', () => {
      expect(() => component.ngOnDestroy()).not.toThrow();
    });

    it('should request the details of the new id', () => {
      expect(service.getTabs).toHaveBeenCalledWith(2);
    });

    it('should should unsubscribe on deselect', () => {
      setSelection(undefined);
      expect(old.unsubscribed).toBe(true);
      expect(getReload()).toBe(undefined); // Cleared timer subscription
    });
  });
});
