import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { ChartsModule } from 'ng2-charts';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { ComponentsModule } from '../../../shared/components/components.module';
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
      BrowserAnimationsModule,
      FormsModule,
      NgxDatatableModule,
      RouterTestingModule,
      ComponentsModule,
      BsDropdownModule.forRoot(),
      ChartsModule,
      PipesModule
    ],
    declarations: [RbdConfigurationListComponent, TableComponent],
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
    component.ngOnChanges();

    expect(component.data.length).toBe(1);
    expect(component.data.pop()).toBe(realOption);
  });

  it('should filter the source column by its piped value', () => {
    const poolConfTable = component.poolConfTable;
    poolConfTable.data = [
      {
        name: 'rbd_qos_read_iops_burst',
        source: 0,
        value: '50'
      },
      {
        name: 'rbd_qos_read_iops_limit',
        source: 1,
        value: '50'
      },
      {
        name: 'rbd_qos_write_iops_limit',
        source: 0,
        value: '100'
      },
      {
        name: 'rbd_qos_write_iops_burst',
        source: 2,
        value: '100'
      }
    ];
    const filter = (keyword: string) => {
      poolConfTable.search = keyword;
      poolConfTable.updateFilter();
      return poolConfTable.rows;
    };
    expect(filter('').length).toBe(4);
    expect(filter('source:global').length).toBe(2);
    expect(filter('source:pool').length).toBe(1);
    expect(filter('source:image').length).toBe(1);
    expect(filter('source:zero').length).toBe(0);
  });
});
