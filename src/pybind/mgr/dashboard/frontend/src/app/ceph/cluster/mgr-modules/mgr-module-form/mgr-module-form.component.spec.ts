import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { MgrModuleFormComponent } from './mgr-module-form.component';

describe('MgrModuleFormComponent', () => {
  let component: MgrModuleFormComponent;
  let fixture: ComponentFixture<MgrModuleFormComponent>;

  configureTestBed({
    declarations: [MgrModuleFormComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot()
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MgrModuleFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('getValidators', () => {
    it('should return ip validator for type addr', () => {
      const result = component.getValidators({ type: 'addr' });
      expect(result.length).toBe(1);
    });

    it('should return required validator for types uint, int, size, secs', () => {
      const types = ['uint', 'int', 'size', 'secs'];
      types.forEach((type) => {
        const result = component.getValidators({ type: type });
        expect(result.length).toBe(1);
      });
    });

    it('should return required, decimalNumber validators for type float', () => {
      const result = component.getValidators({ type: 'float' });
      expect(result.length).toBe(2);
    });

    it('should return uuid validator for type uuid', () => {
      const result = component.getValidators({ type: 'uuid' });
      expect(result.length).toBe(1);
    });

    it('should return no validator for type str', () => {
      const result = component.getValidators({ type: 'str' });
      expect(result.length).toBe(0);
    });

    it('should return min validator for type str', () => {
      const result = component.getValidators({ type: 'str', min: 1 });
      expect(result.length).toBe(1);
    });

    it('should return min, max validators for type str', () => {
      const result = component.getValidators({ type: 'str', min: 1, max: 127 });
      expect(result.length).toBe(2);
    });
  });
});
