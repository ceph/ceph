import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { NfsDetailsComponent } from './nfs-details.component';

describe('NfsDetailsComponent', () => {
  let component: NfsDetailsComponent;
  let fixture: ComponentFixture<NfsDetailsComponent>;

  const elem = (css: string) => fixture.debugElement.query(By.css(css));

  configureTestBed({
    declarations: [NfsDetailsComponent],
    imports: [BrowserAnimationsModule, SharedModule, HttpClientTestingModule, NgbNavModule],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsDetailsComponent);
    component = fixture.componentInstance;

    component.selection = undefined;
    component.selection = {
      export_id: 1,
      path: '/qwe',
      fsal: { name: 'CEPH', user_id: 'fs', fs_name: 1 },
      cluster_id: 'cluster1',
      daemons: ['node1', 'node2'],
      pseudo: '/qwe',
      tag: 'asd',
      access_type: 'RW',
      squash: 'no_root_squash',
      protocols: [3, 4],
      transports: ['TCP', 'UDP'],
      clients: [
        {
          addresses: ['192.168.0.10', '192.168.1.0/8'],
          access_type: 'RW',
          squash: 'root_id_squash'
        }
      ],
      id: 'cluster1:1',
      state: 'LOADING'
    };
    component.ngOnChanges();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component.data).toBeTruthy();
  });

  it('should prepare data', () => {
    expect(component.data).toEqual({
      'Access Type': 'RW',
      'CephFS Filesystem': 1,
      'CephFS User': 'fs',
      Cluster: 'cluster1',
      Daemons: ['node1', 'node2'],
      'NFS Protocol': ['NFSv3', 'NFSv4'],
      Path: '/qwe',
      Pseudo: '/qwe',
      'Security Label': undefined,
      Squash: 'no_root_squash',
      'Storage Backend': 'CephFS',
      Transport: ['TCP', 'UDP']
    });
  });

  it('should prepare data if RGW', () => {
    const newData = _.assignIn(component.selection, {
      fsal: {
        name: 'RGW',
        rgw_user_id: 'rgw_user_id'
      }
    });
    component.selection = newData;
    component.ngOnChanges();
    expect(component.data).toEqual({
      'Access Type': 'RW',
      Cluster: 'cluster1',
      Daemons: ['node1', 'node2'],
      'NFS Protocol': ['NFSv3', 'NFSv4'],
      'Object Gateway User': 'rgw_user_id',
      Path: '/qwe',
      Pseudo: '/qwe',
      Squash: 'no_root_squash',
      'Storage Backend': 'Object Gateway',
      Transport: ['TCP', 'UDP']
    });
  });

  it('should have 1 client', () => {
    expect(elem('ul.nav-tabs li:nth-of-type(2) a').nativeElement.textContent).toBe('Clients (1)');
    expect(component.clients).toEqual([
      {
        access_type: 'RW',
        addresses: ['192.168.0.10', '192.168.1.0/8'],
        squash: 'root_id_squash'
      }
    ]);
  });
});
