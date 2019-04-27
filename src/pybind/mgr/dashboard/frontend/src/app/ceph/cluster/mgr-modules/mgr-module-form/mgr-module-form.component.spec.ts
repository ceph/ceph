import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
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
      ToastModule.forRoot()
    ],
    providers: i18nProviders
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

    it('should return number, required validators for types uint, int, size, secs', () => {
      const types = ['uint', 'int', 'size', 'secs'];
      types.forEach((type) => {
        const result = component.getValidators({ type: type });
        expect(result.length).toBe(2);
      });
    });

    it('should return number, required, min validators for types uint, int, size, secs', () => {
      const types = ['uint', 'int', 'size', 'secs'];
      types.forEach((type) => {
        const result = component.getValidators({ type: type, min: 2 });
        expect(result.length).toBe(3);
      });
    });

    it('should return number, required, min, max validators for types uint, int, size, secs', () => {
      const types = ['uint', 'int', 'size', 'secs'];
      types.forEach((type) => {
        const result = component.getValidators({ type: type, min: 2, max: 5 });
        expect(result.length).toBe(4);
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
