import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { SharedModule } from '../../../shared/shared.module';
import { RgwUserSubuserModalComponent } from './rgw-user-subuser-modal.component';

describe('RgwUserSubuserModalComponent', () => {
  let component: RgwUserSubuserModalComponent;
  let fixture: ComponentFixture<RgwUserSubuserModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RgwUserSubuserModalComponent ],
      imports: [
        ReactiveFormsModule,
        SharedModule
      ],
      providers: [ BsModalRef ]
    })
    .compileComponents();
  }));

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
        {id: 'Edith', permissions: 'full-control'},
        {id: 'Edith:images', permissions: 'read-write'}
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
