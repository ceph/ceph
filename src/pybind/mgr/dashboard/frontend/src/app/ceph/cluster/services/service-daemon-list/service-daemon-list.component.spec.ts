import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import * as _ from 'lodash';
import { of } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { CoreModule } from '../../../../core/core.module';
import { CephServiceService } from '../../../../shared/api/ceph-service.service';
import { HostService } from '../../../../shared/api/host.service';
import { CdTableFetchDataContext } from '../../../../shared/models/cd-table-fetch-data-context';
import { SharedModule } from '../../../../shared/shared.module';
import { CephModule } from '../../../ceph.module';
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
      version: '15.1.0-1174-g16a11f7',
      status: 1,
      status_desc: 'running',
      last_refresh: '2020-02-25T04:33:26.465699'
    },
    {
      hostname: 'osd0',
      container_id: 'baeec41a01374b3ed41016d542d19aef4a70d69c27274f271e26381a0cc58e7a',
      container_image_id: 'e70344c77bcbf3ee389b9bf5128f635cf95f3d59e005c5d8e67fc19bcc74ed23',
      container_image_name: 'docker.io/ceph/daemon-base:latest-master-devel',
      daemon_id: '4',
      daemon_type: 'osd',
      version: '15.1.0-1174-g16a11f7',
      status: 1,
      status_desc: 'running',
      last_refresh: '2020-02-25T04:33:26.465822'
    },
    {
      hostname: 'osd0',
      container_id: '8483de277e365bea4365cee9e1f26606be85c471e4da5d51f57e4b85a42c616e',
      container_image_id: 'e70344c77bcbf3ee389b9bf5128f635cf95f3d59e005c5d8e67fc19bcc74ed23',
      container_image_name: 'docker.io/ceph/daemon-base:latest-master-devel',
      daemon_id: '5',
      daemon_type: 'osd',
      version: '15.1.0-1174-g16a11f7',
      status: 1,
      status_desc: 'running',
      last_refresh: '2020-02-25T04:33:26.465886'
    },
    {
      hostname: 'mon0',
      container_id: '6ca0574f47e300a6979eaf4e7c283a8c4325c2235ae60358482fc4cd58844a21',
      container_image_id: 'e70344c77bcbf3ee389b9bf5128f635cf95f3d59e005c5d8e67fc19bcc74ed23',
      container_image_name: 'docker.io/ceph/daemon-base:latest-master-devel',
      daemon_id: 'a',
      daemon_type: 'mon',
      version: '15.1.0-1174-g16a11f7',
      status: 1,
      status_desc: 'running',
      last_refresh: '2020-02-25T04:33:26.465886'
    }
  ];

  const getDaemonsByHostname = (hostname?: string) => {
    return hostname ? _.filter(daemons, { hostname: hostname }) : daemons;
  };

  const getDaemonsByServiceName = (serviceName?: string) => {
    return serviceName ? _.filter(daemons, { daemon_type: serviceName }) : daemons;
  };

  configureTestBed({
    imports: [HttpClientTestingModule, CephModule, CoreModule, SharedModule],
    declarations: [],
    providers: [i18nProviders]
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
});
