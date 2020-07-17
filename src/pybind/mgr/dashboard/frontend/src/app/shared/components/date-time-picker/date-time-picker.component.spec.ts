import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { FormControl, FormsModule } from '@angular/forms';

import { NgbDatepickerModule, NgbTimepickerModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { DateTimePickerComponent } from './date-time-picker.component';

describe('DateTimePickerComponent', () => {
  let component: DateTimePickerComponent;
  let fixture: ComponentFixture<DateTimePickerComponent>;

  configureTestBed({
    declarations: [DateTimePickerComponent],
    imports: [NgbDatepickerModule, NgbTimepickerModule, FormsModule]
  });

  beforeEach(() => {
    spyOn(Date, 'now').and.returnValue(new Date('2022-02-22T00:00:00.00'));
    fixture = TestBed.createComponent(DateTimePickerComponent);
    component = fixture.componentInstance;
  });

  it('should create with correct datetime', fakeAsync(() => {
    component.control = new FormControl('2022-02-26 00:00:00');
    fixture.detectChanges();
    tick();
    expect(component).toBeTruthy();
    expect(component.control.value).toBe('2022-02-26 00:00:00');
  }));

  it('should update control value if datetime is not valid', fakeAsync(() => {
    component.control = new FormControl('not valid');
    fixture.detectChanges();
    tick();
    expect(component.control.value).toBe('2022-02-22 00:00:00');
  }));

  it('should init with only date enabled', () => {
    component.control = new FormControl();
    component.hasTime = false;
    fixture.detectChanges();
    expect(component.format).toBe('YYYY-MM-DD');
  });

  it('should init with time enabled', () => {
    component.control = new FormControl();
    component.hasSeconds = false;
    fixture.detectChanges();
    expect(component.format).toBe('YYYY-MM-DD HH:mm');
  });

  it('should init with seconds enabled', () => {
    component.control = new FormControl();
    fixture.detectChanges();
    expect(component.format).toBe('YYYY-MM-DD HH:mm:ss');
  });
});
