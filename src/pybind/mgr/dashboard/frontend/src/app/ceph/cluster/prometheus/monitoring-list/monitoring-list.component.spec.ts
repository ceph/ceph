import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';

import { i18nProviders } from '../../../../../testing/unit-test-helper';
import { AuthModule } from '../../../../core/auth/auth.module';
import { CoreModule } from '../../../../core/core.module';
import { CephfsModule } from '../../../cephfs/cephfs.module';
import { DashboardModule } from '../../../dashboard/dashboard.module';
import { NfsModule } from '../../../nfs/nfs.module';
import { ClusterModule } from '../../cluster.module';
import { MonitoringListComponent } from './monitoring-list.component';

describe('MonitoringListComponent', () => {
  let component: MonitoringListComponent;
  let fixture: ComponentFixture<MonitoringListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        ClusterModule,
        DashboardModule,
        CephfsModule,
        AuthModule,
        NfsModule,
        CoreModule,
        ToastrModule.forRoot(),
        HttpClientTestingModule
      ],
      declarations: [],
      providers: [i18nProviders]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MonitoringListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
