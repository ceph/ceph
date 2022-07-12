import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxPipeFunctionModule } from 'ngx-pipe-function';
import { ToastrModule } from 'ngx-toastr';

import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { SummaryService } from '~/app/shared/services/summary.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ServiceDaemonListComponent } from '../service-daemon-list/service-daemon-list.component';
import { ServiceDetailsComponent } from './service-details.component';

describe('ServiceDetailsComponent', () => {
  let component: ServiceDetailsComponent;
  let fixture: ComponentFixture<ServiceDetailsComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      NgbNavModule,
      NgxPipeFunctionModule,
      ToastrModule.forRoot()
    ],
    declarations: [ServiceDetailsComponent, ServiceDaemonListComponent],
    providers: [{ provide: SummaryService, useValue: { subscribeOnce: jest.fn() } }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ServiceDetailsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });
});
