import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { DataTableModule } from '../../../../shared/datatable/datatable.module';
import { ConfigurationDetailsComponent } from './configuration-details.component';

describe('ConfigurationDetailsComponent', () => {
  let component: ConfigurationDetailsComponent;
  let fixture: ComponentFixture<ConfigurationDetailsComponent>;

  configureTestBed({
    declarations: [ConfigurationDetailsComponent],
    imports: [DataTableModule, TabsModule.forRoot()]
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
