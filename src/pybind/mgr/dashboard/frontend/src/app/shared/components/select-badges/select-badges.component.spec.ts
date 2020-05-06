import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule, Validators } from '@angular/forms';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { PopoverModule } from 'ngx-bootstrap/popover';
import { TooltipModule } from 'ngx-bootstrap/tooltip';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SelectMessages } from '../select/select-messages.model';
import { SelectComponent } from '../select/select.component';
import { SelectBadgesComponent } from './select-badges.component';

describe('SelectBadgesComponent', () => {
  let component: SelectBadgesComponent;
  let fixture: ComponentFixture<SelectBadgesComponent>;

  configureTestBed({
    declarations: [SelectBadgesComponent, SelectComponent],
    imports: [PopoverModule.forRoot(), TooltipModule, ReactiveFormsModule],
    providers: i18nProviders
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
    const i18n = TestBed.inject(I18n);
    const messages = new SelectMessages({ empty: 'foo bar' }, i18n);
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
