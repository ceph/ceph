import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { RgwUserSubuserModalComponent } from './rgw-user-subuser-modal.component';

describe('RgwUserSubuserModalComponent', () => {
  let component: RgwUserSubuserModalComponent;
  let fixture: ComponentFixture<RgwUserSubuserModalComponent>;

  configureTestBed({
    declarations: [RgwUserSubuserModalComponent],
    imports: [ReactiveFormsModule, SharedModule, RouterTestingModule],
    providers: [NgbActiveModal, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserSubuserModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('subuserValidator', () => {
    beforeEach(() => {
      component.editing = false;
      component.subusers = [
        { id: 'Edith', permissions: 'full-control' },
        { id: 'Edith:images', permissions: 'read-write' }
      ];
    });

    it('should validate subuser (1/5)', () => {
      component.editing = true;
      const validatorFn = component.subuserValidator();
      const resp = validatorFn(new FormControl());
      expect(resp).toBe(null);
    });

    it('should validate subuser (2/5)', () => {
      const validatorFn = component.subuserValidator();
      const resp = validatorFn(new FormControl(''));
      expect(resp).toBe(null);
    });

    it('should validate subuser (3/5)', () => {
      const validatorFn = component.subuserValidator();
      const resp = validatorFn(new FormControl('Melissa'));
      expect(resp).toBe(null);
    });

    it('should validate subuser (4/5)', () => {
      const validatorFn = component.subuserValidator();
      const resp = validatorFn(new FormControl('Edith'));
      expect(resp.subuserIdExists).toBeTruthy();
    });

    it('should validate subuser (5/5)', () => {
      const validatorFn = component.subuserValidator();
      const resp = validatorFn(new FormControl('images'));
      expect(resp.subuserIdExists).toBeTruthy();
    });
  });
});
