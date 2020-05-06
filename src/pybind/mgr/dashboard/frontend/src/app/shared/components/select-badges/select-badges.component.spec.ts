import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule, Validators } from '@angular/forms';

import { NgbPopoverModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SelectMessages } from '../select/select-messages.model';
import { SelectComponent } from '../select/select.component';
import { SelectBadgesComponent } from './select-badges.component';

describe('SelectBadgesComponent', () => {
  let component: SelectBadgesComponent;
  let fixture: ComponentFixture<SelectBadgesComponent>;

  configureTestBed({
    declarations: [SelectBadgesComponent, SelectComponent],
    imports: [NgbPopoverModule, NgbTooltipModule, ReactiveFormsModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SelectBadgesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should reflect the attributes into CdSelect', () => {
    const data = ['a', 'b'];
    const options = [
      { name: 'option1', description: '', selected: false, enabled: true },
      { name: 'option2', description: '', selected: false, enabled: true }
    ];
    const messages = new SelectMessages({ empty: 'foo bar' });
    const selectionLimit = 2;
    const customBadges = true;
    const customBadgeValidators = [Validators.required];

    component.data = data;
    component.options = options;
    component.messages = messages;
    component.selectionLimit = selectionLimit;
    component.customBadges = customBadges;
    component.customBadgeValidators = customBadgeValidators;

    fixture.detectChanges();

    expect(component.cdSelect.data).toEqual(data);
    expect(component.cdSelect.options).toEqual(options);
    expect(component.cdSelect.messages).toEqual(messages);
    expect(component.cdSelect.selectionLimit).toEqual(selectionLimit);
    expect(component.cdSelect.customBadges).toEqual(customBadges);
    expect(component.cdSelect.customBadgeValidators).toEqual(customBadgeValidators);
  });
});
