import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import _ from 'lodash';
import { NgxPipeFunctionModule } from 'ngx-pipe-function';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { CephModule } from '~/app/ceph/ceph.module';
import { CoreModule } from '~/app/core/core.module';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { HostService } from '~/app/shared/api/host.service';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ServiceDaemonListComponent } from './service-daemon-list.component';

describe('ServiceDaemonListComponent', () => {
  let component: ServiceDaemonListComponent;
  let fixture: ComponentFixture<ServiceDaemonListComponent>;

  const daemons = [
    {
      hostname: 'osd0',
      container_id: '003c10beafc8c27b635bcdfed1ed832e4c1005be89bb1bb05ad4cc6c2b98e41b',
      container_image_id: 'e70344c77bcbf3ee389b9bf5128f635cf95f3d59e005c5d8e67fc19bcc74ed23',
      container_image_name: 'docker.io/ceph/daemon-base:latest-master-devel',
      daemon_id: '3',
      daemon_type: 'osd',
      daemon_name: 'osd.3',
      version: '15.1.0-1174-g16a11f7',
      memory_usage: '17.7',
      cpu_percentage: '3.54%',
      status: 1,
      status_desc: 'running',
      last_refresh: '2020-02-25T04:33:26.465699',
      events: [
        { created: '2020-02-24T04:33:26.465699' },
        { created: '2020-02-25T04:33:26.465699' },
        { created: '2020-02-26T04:33:26.465699' }
      ]
    },
    {
      hostname: 'osd0',
      container_id: 'baeec41a01374b3ed41016d542d19aef4a70d69c27274f271e26381a0cc58e7a',
      container_image_id: 'e70344c77bcbf3ee389b9bf5128f635cf95f3d59e005c5d8e67fc19bcc74ed23',
      container_image_name: 'docker.io/ceph/daemon-base:latest-master-devel',
      daemon_id: '4',
      daemon_type: 'osd',
      daemon_name: 'osd.4',
      version: '15.1.0-1174-g16a11f7',
      memory_usage: '17.7',
      cpu_percentage: '3.54%',
      status: 1,
      status_desc: 'running',
      last_refresh: '2020-02-25T04:33:26.465822',
      events: []
    },
    {
      hostname: 'osd0',
      container_id: '8483de277e365bea4365cee9e1f26606be85c471e4da5d51f57e4b85a42c616e',
      container_image_id: 'e70344c77bcbf3ee389b9bf5128f635cf95f3d59e005c5d8e67fc19bcc74ed23',
      container_image_name: 'docker.io/ceph/daemon-base:latest-master-devel',
      daemon_id: '5',
      daemon_type: 'osd',
      daemon_name: 'osd.5',
      version: '15.1.0-1174-g16a11f7',
      memory_usage: '17.7',
      cpu_percentage: '3.54%',
      status: 1,
      status_desc: 'running',
      last_refresh: '2020-02-25T04:33:26.465886',
      events: []
    },
    {
      hostname: 'mon0',
      container_id: '6ca0574f47e300a6979eaf4e7c283a8c4325c2235ae60358482fc4cd58844a21',
      container_image_id: 'e70344c77bcbf3ee389b9bf5128f635cf95f3d59e005c5d8e67fc19bcc74ed23',
      container_image_name: 'docker.io/ceph/daemon-base:latest-master-devel',
      daemon_id: 'a',
      daemon_name: 'mon.a',
      daemon_type: 'mon',
      version: '15.1.0-1174-g16a11f7',
      memory_usage: '17.7',
      cpu_percentage: '3.54%',
      status: 1,
      status_desc: 'running',
      last_refresh: '2020-02-25T04:33:26.465886',
      events: []
    }
  ];

  const services = [
    {
      service_type: 'osd',
      service_name: 'osd',
      status: {
        container_image_id: 'e70344c77bcbf3ee389b9bf5128f635cf95f3d59e005c5d8e67fc19bcc74ed23',
        container_image_name: 'docker.io/ceph/daemon-base:latest-master-devel',
        size: 3,
        running: 3,
        last_refresh: '2020-02-25T04:33:26.465699'
      },
      events: '2021-03-22T07:34:48.582163Z service:osd [INFO] "service was created"'
    },
    {
      service_type: 'crash',
      service_name: 'crash',
      status: {
        container_image_id: 'e70344c77bcbf3ee389b9bf5128f635cf95f3d59e005c5d8e67fc19bcc74ed23',
        container_image_name: 'docker.io/ceph/daemon-base:latest-master-devel',
        size: 1,
        running: 1,
        last_refresh: '2020-02-25T04:33:26.465766'
      },
      events: '2021-03-22T07:34:48.582163Z service:osd [INFO] "service was created"'
    }
  ];

  const getDaemonsByHostname = (hostname?: string) => {
    return hostname ? _.filter(daemons, { hostname: hostname }) : daemons;
  };

  const getDaemonsByServiceName = (serviceName?: string) => {
    return serviceName ? _.filter(daemons, { daemon_type: serviceName }) : daemons;
  };

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      CephModule,
      CoreModule,
      NgxPipeFunctionModule,
      SharedModule,
      ToastrModule.forRoot()
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ServiceDaemonListComponent);
    component = fixture.componentInstance;
    const hostService = TestBed.inject(HostService);
    const cephServiceService = TestBed.inject(CephServiceService);
    spyOn(hostService, 'getDaemons').and.callFake(() =>
      of(getDaemonsByHostname(component.hostname))
    );
    spyOn(cephServiceService, 'getDaemons').and.callFake(() =>
      of(getDaemonsByServiceName(component.serviceName))
    );
    spyOn(cephServiceService, 'list').and.returnValue(of(services));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should list daemons by host', () => {
    component.hostname = 'mon0';
    component.getDaemons(new CdTableFetchDataContext(() => undefined));
    expect(component.daemons.length).toBe(1);
  });

  it('should list daemons by service', () => {
    component.serviceName = 'osd';
    component.getDaemons(new CdTableFetchDataContext(() => undefined));
    expect(component.daemons.length).toBe(3);
  });

  it('should list services', () => {
    component.getServices(new CdTableFetchDataContext(() => undefined));
    expect(component.services.length).toBe(2);
  });

  it('should not display doc panel if orchestrator is available', () => {
    expect(component.showDocPanel).toBeFalsy();
  });

  it('should call daemon action', () => {
    const daemon = daemons[0];
    component.selection.selected = [daemon];
    component['daemonService'].action = jest.fn(() => of());
    for (const action of ['start', 'stop', 'restart', 'redeploy']) {
      component.daemonAction(action);
      expect(component['daemonService'].action).toHaveBeenCalledWith(daemon.daemon_name, action);
    }
  });

  it('should disable daemon actions', () => {
    const daemon = {
      daemon_type: 'osd',
      status_desc: 'running'
    };

    const states = {
      start: true,
      stop: false,
      restart: false,
      redeploy: false
    };
    const expectBool = (toExpect: boolean, arg: boolean) => {
      if (toExpect === true) {
        expect(arg).toBeTruthy();
      } else {
        expect(arg).toBeFalsy();
      }
    };

    component.selection.selected = [daemon];
    for (const action of ['start', 'stop', 'restart', 'redeploy']) {
      expectBool(states[action], component.actionDisabled(action));
    }

    daemon.status_desc = 'stopped';
    states.start = false;
    states.stop = true;
    component.selection.selected = [daemon];
    for (const action of ['start', 'stop', 'restart', 'redeploy']) {
      expectBool(states[action], component.actionDisabled(action));
    }
  });

  it('should disable daemon actions in mgr and mon daemon', () => {
    const daemon = {
      daemon_type: 'mgr',
      status_desc: 'running'
    };
    for (const action of ['start', 'stop', 'restart', 'redeploy']) {
      expect(component.actionDisabled(action)).toBeTruthy();
    }
    daemon.daemon_type = 'mon';
    for (const action of ['start', 'stop', 'restart', 'redeploy']) {
      expect(component.actionDisabled(action)).toBeTruthy();
    }
  });

  it('should disable daemon actions if no selection', () => {
    component.selection.selected = [];
    for (const action of ['start', 'stop', 'restart', 'redeploy']) {
      expect(component.actionDisabled(action)).toBeTruthy();
    }
  });

  it('should sort daemons events', () => {
    component.sortDaemonEvents();
    const daemon = daemons[0];
    for (let i = 1; i < daemon.events.length; i++) {
      const t1 = new Date(daemon.events[i - 1].created).getTime();
      const t2 = new Date(daemon.events[i].created).getTime();
      expect(t1 >= t2).toBeTruthy();
    }
  });
});
