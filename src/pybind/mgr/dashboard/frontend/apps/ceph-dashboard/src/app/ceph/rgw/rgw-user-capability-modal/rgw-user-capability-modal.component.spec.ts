import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwUserCapabilityModalComponent } from './rgw-user-capability-modal.component';
import { SelectModule } from 'carbon-components-angular';

describe('RgwUserCapabilityModalComponent', () => {
  let component: RgwUserCapabilityModalComponent;
  let fixture: ComponentFixture<RgwUserCapabilityModalComponent>;

  configureTestBed({
    declarations: [RgwUserCapabilityModalComponent],
    imports: [ReactiveFormsModule, SharedModule, RouterTestingModule, SelectModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserCapabilityModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
