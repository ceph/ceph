import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import * as _ from 'lodash';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { NfsDetailsComponent } from './nfs-details.component';

describe('NfsDetailsComponent', () => {
  let component: NfsDetailsComponent;
  let fixture: ComponentFixture<NfsDetailsComponent>;

  configureTestBed({
    declarations: [NfsDetailsComponent],
    imports: [SharedModule, TabsModule.forRoot(), HttpClientTestingModule],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsDetailsComponent);
    component = fixture.componentInstance;

    component.selection = new CdTableSelection();
    component.selection.selected = [
      {
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
        clients: [],
        id: 'cluster1:1',
        state: 'LOADING'
      }
    ];
    component.selection.update();

    fixture.detectChanges();
  });

  beforeEach(() => {});

  it('should create', () => {
    component.ngOnChanges();
    expect(component.data).toBeTruthy();
  });

  it('should prepare data', () => {
    component.ngOnChanges();
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
    const newData = _.assignIn(component.selection.first(), {
      fsal: {
        name: 'RGW',
        rgw_user_id: 'rgw_user_id'
      }
    });
    component.selection.selected = [newData];
    component.selection.update();
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
});
