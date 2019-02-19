import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Component, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { SettingDetailsComponent } from '../setting-details/setting-details.component';
import { SettingsViewComponent } from './settings-view.component';

@Component({ selector: 'cd-cephfs-detail', template: '' })
class SettingsDetailStubComponent {
  @Input()
  selection: CdTableSelection;
}

describe('SettingsViewComponent', () => {
  let component: SettingsViewComponent;
  let fixture: ComponentFixture<SettingsViewComponent>;

  configureTestBed({
    declarations: [SettingsViewComponent, SettingDetailsComponent, SettingsDetailStubComponent],
    imports: [SharedModule, TabsModule.forRoot(), ToastrModule.forRoot(), HttpClientTestingModule],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SettingsViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
