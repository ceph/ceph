import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { MgrModuleDetailsComponent } from './mgr-module-details.component';

describe('MgrModuleDetailsComponent', () => {
  let component: MgrModuleDetailsComponent;
  let fixture: ComponentFixture<MgrModuleDetailsComponent>;

  configureTestBed({
    declarations: [MgrModuleDetailsComponent],
    imports: [HttpClientTestingModule, SharedModule, NgbNavModule],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MgrModuleDetailsComponent);
    component = fixture.componentInstance;
    component.selection = undefined;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
