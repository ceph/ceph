import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MgrModuleDetailsComponent } from './mgr-module-details.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';

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
