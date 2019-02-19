import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { AppModule } from '../../../app.module';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SettingDetailsComponent } from './setting-details.component';

describe('SettingDetailsComponent', () => {
  let component: SettingDetailsComponent;
  let fixture: ComponentFixture<SettingDetailsComponent>;

  configureTestBed({
    imports: [AppModule],
    decalarations: [SettingDetailsComponent],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SettingDetailsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
