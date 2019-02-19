import { SimpleChange } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { ChartsModule } from 'ng2-charts';
import { AlertModule } from 'ngx-bootstrap/alert';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { ErrorPanelComponent } from '../../../shared/components/error-panel/error-panel.component';
import { SparklineComponent } from '../../../shared/components/sparkline/sparkline.component';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { RbdConfigurationEntry } from '../../../shared/models/configuration';
import { PipesModule } from '../../../shared/pipes/pipes.module';
import { FormatterService } from '../../../shared/services/formatter.service';
import { RbdConfigurationService } from '../../../shared/services/rbd-configuration.service';
import { RbdConfigurationListComponent } from './rbd-configuration-list.component';

describe('RbdConfigurationListComponent', () => {
  let component: RbdConfigurationListComponent;
  let fixture: ComponentFixture<RbdConfigurationListComponent>;

  configureTestBed({
    imports: [
      FormsModule,
      NgxDatatableModule,
      RouterTestingModule,
      AlertModule,
      ChartsModule,
      PipesModule
    ],
    declarations: [
      RbdConfigurationListComponent,
      TableComponent,
      ErrorPanelComponent,
      SparklineComponent
    ],
    providers: [FormatterService, RbdConfigurationService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdConfigurationListComponent);
    component = fixture.componentInstance;
    component.data = [];
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('filters options out which are not defined in RbdConfigurationService', () => {
    const fakeOption = { name: 'foo', source: 0, value: '50' } as RbdConfigurationEntry;
    const realOption = {
      name: 'rbd_qos_read_iops_burst',
      source: 0,
      value: '50'
    } as RbdConfigurationEntry;

    component.data = [fakeOption, realOption];
    component.ngOnChanges({ name: new SimpleChange(null, null, null) });

    expect(component.data.length).toBe(1);
    expect(component.data.pop()).toBe(realOption);
  });
});
