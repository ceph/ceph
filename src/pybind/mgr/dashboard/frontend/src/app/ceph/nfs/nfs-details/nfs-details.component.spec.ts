import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NfsDetailsComponent } from './nfs-details.component';

describe('NfsDetailsComponent', () => {
  let component: NfsDetailsComponent;
  let fixture: ComponentFixture<NfsDetailsComponent>;

  const elem = (css: string) => fixture.debugElement.query(By.css(css));

  configureTestBed({
    declarations: [NfsDetailsComponent],
    imports: [BrowserAnimationsModule, SharedModule, HttpClientTestingModule, NgbNavModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsDetailsComponent);
    component = fixture.componentInstance;

    component.selection = {
      export_id: 1,
      path: '/qwe',
      fsal: { name: 'CEPH', user_id: 'fs', fs_name: 1 },
      cluster_id: 'cluster1',
      pseudo: '/qwe',
      access_type: 'RW',
      squash: 'no_root_squash',
      protocols: [4],
      transports: ['TCP', 'UDP'],
      clients: [
        {
          addresses: ['192.168.0.10', '192.168.1.0/8'],
          access_type: 'RW',
          squash: 'root_id_squash'
        }
      ]
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
      'NFS Protocol': ['NFSv4'],
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
        user_id: 'user-id'
      }
    });
    component.selection = newData;
    component.ngOnChanges();
    expect(component.data).toEqual({
      'Access Type': 'RW',
      Cluster: 'cluster1',
      'NFS Protocol': ['NFSv4'],
      'Object Gateway User': 'user-id',
      Path: '/qwe',
      Pseudo: '/qwe',
      Squash: 'no_root_squash',
      'Storage Backend': 'Object Gateway',
      Transport: ['TCP', 'UDP']
    });
  });

  it('should have 1 client', () => {
    expect(elem('nav.nav-tabs a:nth-of-type(2)').nativeElement.textContent).toBe('Clients (1)');
    expect(component.clients).toEqual([
      {
        access_type: 'RW',
        addresses: ['192.168.0.10', '192.168.1.0/8'],
        squash: 'root_id_squash'
      }
    ]);
  });
});
