import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { MgrModuleDetailsComponent } from './mgr-module-details.component';

describe('MgrModuleDetailsComponent', () => {
  let component: MgrModuleDetailsComponent;
  let fixture: ComponentFixture<MgrModuleDetailsComponent>;

  configureTestBed({
    declarations: [MgrModuleDetailsComponent],
    imports: [HttpClientTestingModule, SharedModule]
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
