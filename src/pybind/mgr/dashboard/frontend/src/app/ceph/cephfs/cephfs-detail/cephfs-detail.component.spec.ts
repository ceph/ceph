import { Component, Input } from '@angular/core';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ChartsModule } from 'ng2-charts/ng2-charts';
import { BsDropdownModule, ProgressbarModule, TabsModule } from 'ngx-bootstrap';
import { Observable } from 'rxjs/Observable';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { SharedModule } from '../../../shared/shared.module';
import { CephfsChartComponent } from '../cephfs-chart/cephfs-chart.component';
import { ClientsComponent } from '../clients/clients.component';
import { CephfsDetailComponent } from './cephfs-detail.component';

@Component({ selector: 'cd-cephfs-chart', template: '' })
class CephfsChartStubComponent {
  @Input() mdsCounter: any;
}

@Component({ selector: 'cd-clients', template: '' })
class ClientsStubComponent {
  @Input() mdsCounter: any;
}

describe('CephfsDetailComponent', () => {
  let component: CephfsDetailComponent;
  let fixture: ComponentFixture<CephfsDetailComponent>;

  const fakeFilesystemService = {
    getCephfs: (id) => {
      return Observable.create((observer) => {
        return () => console.log('disposed');
      });
    },
    getMdsCounters: (id) => {
      return Observable.create((observer) => {
        return () => console.log('disposed');
      });
    }
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        SharedModule,
        ChartsModule,
        RouterTestingModule,
        BsDropdownModule.forRoot(),
        ProgressbarModule.forRoot(),
        TabsModule.forRoot()
      ],
      declarations: [CephfsDetailComponent, CephfsChartStubComponent, ClientsStubComponent],
      providers: [{ provide: CephfsService, useValue: fakeFilesystemService }]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
