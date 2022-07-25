import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BehaviorSubject, of } from 'rxjs';

import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CardComponent } from '../card/card.component';
import { DashboardComponent } from './dashboard.component';

export class SummaryServiceMock {
  summaryDataSource = new BehaviorSubject({
    version:
      'ceph version 17.0.0-12222-gcd0cd7cb ' +
      '(b8193bb4cda16ccc5b028c3e1df62bc72350a15d) quincy (dev)'
  });
  summaryData$ = this.summaryDataSource.asObservable();

  subscribe(call: any) {
    return this.summaryData$.subscribe(call);
  }
}

describe('CardComponent', () => {
  let component: DashboardComponent;
  let fixture: ComponentFixture<DashboardComponent>;
  let configurationService: ConfigurationService;
  let orchestratorService: MgrModuleService;

  const configValueData: any = {
    value: [
      {
        section: 'mgr',
        value: 'e90a0d58-658e-4148-8f61-e896c86f0696'
      }
    ]
  };

  const orchData: any = {
    log_level: '',
    log_to_cluster: false,
    log_to_cluster_level: 'info',
    log_to_file: false,
    orchestrator: 'cephadm'
  };

  configureTestBed({
    imports: [HttpClientTestingModule],
    declarations: [DashboardComponent, CardComponent],
    providers: [{ provide: SummaryService, useClass: SummaryServiceMock }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DashboardComponent);
    component = fixture.componentInstance;
    configurationService = TestBed.inject(ConfigurationService);
    orchestratorService = TestBed.inject(MgrModuleService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render all cards', () => {
    const dashboardCards = fixture.debugElement.nativeElement.querySelectorAll('cd-card');
    expect(dashboardCards.length).toBe(6);
  });

  it('should get corresponding data into detailsCardData', () => {
    spyOn(configurationService, 'get').and.returnValue(of(configValueData));
    spyOn(orchestratorService, 'getConfig').and.returnValue(of(orchData));
    component.ngOnInit();
    expect(component.detailsCardData.fsid).toBe('e90a0d58-658e-4148-8f61-e896c86f0696');
    expect(component.detailsCardData.orchestrator).toBe('Cephadm');
    expect(component.detailsCardData.cephVersion).toBe('17.0.0-12222-gcd0cd7cb quincy (dev)');
  });
});
