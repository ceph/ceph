import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { DataTableModule } from '../../../../shared/datatable/datatable.module';
import { SharedModule } from '../../../../shared/shared.module';
import { ConfigurationDetailsComponent } from './configuration-details.component';

describe('ConfigurationDetailsComponent', () => {
  let component: ConfigurationDetailsComponent;
  let fixture: ComponentFixture<ConfigurationDetailsComponent>;

  configureTestBed({
    declarations: [ConfigurationDetailsComponent],
    imports: [DataTableModule, SharedModule],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigurationDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
