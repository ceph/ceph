import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { RgwUserCapabilityModalComponent } from './rgw-user-capability-modal.component';

describe('RgwUserCapabilityModalComponent', () => {
  let component: RgwUserCapabilityModalComponent;
  let fixture: ComponentFixture<RgwUserCapabilityModalComponent>;

  configureTestBed({
    declarations: [RgwUserCapabilityModalComponent],
    imports: [ReactiveFormsModule, SharedModule],
    providers: [BsModalRef]
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
