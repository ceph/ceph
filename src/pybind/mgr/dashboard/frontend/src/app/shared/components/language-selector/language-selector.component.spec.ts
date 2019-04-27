import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { listLocales } from 'ngx-bootstrap/chronos';
import { BsLocaleService } from 'ngx-bootstrap/datepicker';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { LanguageSelectorComponent } from './language-selector.component';

describe('LanguageSelectorComponent', () => {
  let component: LanguageSelectorComponent;
  let fixture: ComponentFixture<LanguageSelectorComponent>;

  configureTestBed({
    declarations: [LanguageSelectorComponent],
    providers: [BsLocaleService],
    imports: [FormsModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LanguageSelectorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    spyOn(window.location, 'reload').and.callFake(() => component.ngOnInit());
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should read current language', () => {
    expect(component.selectedLanguage).toBe('en-US');
    expect(listLocales()).toEqual([]);
  });

  const expectLanguageChange = (lang) => {
    component.changeLanguage(lang);
    expect(component.selectedLanguage).toBe(lang);
    expect(listLocales().includes(lang.slice(0, 2))).toBe(true);
  };

  it('should change to cs', () => {
    expectLanguageChange('cs');
  });

  it('should change to de-DE', () => {
    expectLanguageChange('de-DE');
  });

  it('should change to es-ES', () => {
    expectLanguageChange('es-ES');
  });

  it('should change to fr-FR', () => {
    expectLanguageChange('fr-FR');
  });

  it('should change to id-ID', () => {
    expectLanguageChange('id-ID');
  });

  it('should change to pl-PL', () => {
    expectLanguageChange('pl-PL');
  });

  it('should change to pt-PT', () => {
    expectLanguageChange('pt-PT');
  });

  it('should change to zh-CN', () => {
    expectLanguageChange('zh-CN');
  });
});
