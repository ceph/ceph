import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Component, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ChartsModule } from 'ng2-charts/ng2-charts';
import { BsDropdownModule, ProgressbarModule, TabsModule } from 'ngx-bootstrap';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { SharedModule } from '../../../shared/shared.module';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { CephfsDetailComponent } from './cephfs-detail.component';

@Component({ selector: 'cd-cephfs-chart', template: '' })
class CephfsChartStubComponent {
  @Input() mdsCounter: any;
}

@Component({ selector: 'cd-cephfs-clients', template: '' })
class CephfsClientsStubComponent {
  @Input() mdsCounter: any;
}

describe('CephfsDetailComponent', () => {
  let component: CephfsDetailComponent;
  let fixture: ComponentFixture<CephfsDetailComponent>;

  configureTestBed({
    imports: [
      SharedModule,
      ChartsModule,
      RouterTestingModule,
      BsDropdownModule.forRoot(),
      ProgressbarModule.forRoot(),
      TabsModule.forRoot(),
      HttpClientTestingModule
    ],
    declarations: [CephfsDetailComponent, CephfsChartStubComponent, CephfsClientsStubComponent],
    providers: [CephfsService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
