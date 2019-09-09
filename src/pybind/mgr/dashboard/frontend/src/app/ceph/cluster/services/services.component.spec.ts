import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';
import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { OrchestratorService } from '../../../shared/api/orchestrator.service';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { SharedModule } from '../../../shared/shared.module';
import { ServicesComponent } from './services.component';

describe('ServicesComponent', () => {
  let component: ServicesComponent;
  let fixture: ComponentFixture<ServicesComponent>;
  let reqHostname: string;

  const services = [
    {
      nodename: 'host0',
      service: '',
      service_instance: 'x',
      service_type: 'mon'
    },
    {
      nodename: 'host0',
      service: '',
      service_instance: '0',
      service_type: 'osd'
    },
    {
      nodename: 'host1',
      service: '',
      service_instance: 'y',
      service_type: 'mon'
    },
    {
      nodename: 'host1',
      service: '',
      service_instance: '1',
      service_type: 'osd'
    }
  ];

  const getServiceList = (hostname: String) => {
    return hostname ? services.filter((service) => service.nodename === hostname) : services;
  };

  configureTestBed({
    imports: [SharedModule, HttpClientTestingModule, RouterTestingModule],
    providers: [i18nProviders],
    declarations: [ServicesComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ServicesComponent);
    component = fixture.componentInstance;
    const orchService = TestBed.get(OrchestratorService);
    spyOn(orchService, 'status').and.returnValue(of({ available: true }));
    reqHostname = '';
    spyOn(orchService, 'serviceList').and.callFake(() => of(getServiceList(reqHostname)));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have columns that are sortable', () => {
    expect(component.columns.every((column) => Boolean(column.prop))).toBeTruthy();
  });

  it('should return all services', () => {
    component.getServices(new CdTableFetchDataContext(() => {}));
    expect(component.services.length).toBe(4);
  });

  it('should return services on a host', () => {
    reqHostname = 'host0';
    component.getServices(new CdTableFetchDataContext(() => {}));
    expect(component.services.length).toBe(2);
    expect(component.services[0].nodename).toBe(reqHostname);
    expect(component.services[1].nodename).toBe(reqHostname);
  });
});
