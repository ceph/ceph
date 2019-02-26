import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { LocaleHelper } from '../../../locale.helper';
import { LanguageSelectorComponent } from './language-selector.component';

describe('LanguageSelectorComponent', () => {
  let component: LanguageSelectorComponent;
  let fixture: ComponentFixture<LanguageSelectorComponent>;

  configureTestBed({
    declarations: [LanguageSelectorComponent],
    imports: [FormsModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LanguageSelectorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should read current language', () => {
    expect(component.selectedLanguage).toBe('en-US');
  });

  it('should change language', () => {
    // Removes error in jsdom
    window.location.reload = jest.fn();

    expect(LocaleHelper.getLocale()).toBe('en-US');
    component.changeLanguage('pt-PT');
    expect(LocaleHelper.getLocale()).toBe('pt-PT');
  });
});
