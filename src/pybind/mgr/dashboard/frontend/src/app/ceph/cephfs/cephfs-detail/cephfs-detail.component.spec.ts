import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Component, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { ProgressbarModule } from 'ngx-bootstrap/progressbar';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { CephfsDetailComponent } from './cephfs-detail.component';

@Component({ selector: 'cd-cephfs-chart', template: '' })
class CephfsChartStubComponent {
  @Input()
  mdsCounter: any;
}

@Component({ selector: 'cd-cephfs-clients', template: '' })
class CephfsClientsStubComponent {
  @Input()
  mdsCounter: any;
}

describe('CephfsDetailComponent', () => {
  let component: CephfsDetailComponent;
  let fixture: ComponentFixture<CephfsDetailComponent>;

  configureTestBed({
    imports: [
      SharedModule,
      RouterTestingModule,
      BsDropdownModule.forRoot(),
      ProgressbarModule.forRoot(),
      TabsModule.forRoot(),
      HttpClientTestingModule
    ],
    declarations: [CephfsDetailComponent, CephfsChartStubComponent, CephfsClientsStubComponent],
    providers: i18nProviders
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
