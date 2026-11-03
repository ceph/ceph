import { ComponentFixture, TestBed } from '@angular/core/testing';
import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from '@angular/core';

import { FormSectionToggleComponent } from './form-section-toggle.component';

describe('FormSectionToggleComponent', () => {
  let component: FormSectionToggleComponent;
  let fixture: ComponentFixture<FormSectionToggleComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FormSectionToggleComponent],
      schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(FormSectionToggleComponent);
    component = fixture.componentInstance;
    component.showText = 'Show settings';
    component.hideText = 'Hide settings';
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should toggle expanded state', () => {
    const event = new Event('click');
    spyOn(event, 'preventDefault');
    spyOn(component.expandedChange, 'emit');

    component.toggle(event);

    expect(event.preventDefault).toHaveBeenCalled();
    expect(component.expanded).toBe(true);
    expect(component.expandedChange.emit).toHaveBeenCalledWith(true);
  });
});
