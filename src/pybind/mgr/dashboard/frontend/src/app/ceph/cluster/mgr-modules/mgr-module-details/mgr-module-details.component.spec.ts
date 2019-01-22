import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../../shared/shared.module';
import { MgrModuleDetailsComponent } from './mgr-module-details.component';

describe('MgrModuleDetailsComponent', () => {
  let component: MgrModuleDetailsComponent;
  let fixture: ComponentFixture<MgrModuleDetailsComponent>;

  configureTestBed({
    declarations: [MgrModuleDetailsComponent],
    imports: [HttpClientTestingModule, SharedModule, TabsModule.forRoot()],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MgrModuleDetailsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
