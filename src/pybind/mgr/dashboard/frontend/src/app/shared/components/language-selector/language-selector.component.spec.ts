import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { listLocales } from 'ngx-bootstrap/chronos';
import { BsLocaleService } from 'ngx-bootstrap/datepicker';

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed } from '../../../../testing/unit-test-helper';
import { LanguageSelectorComponent } from './language-selector.component';

describe('LanguageSelectorComponent', () => {
  let component: LanguageSelectorComponent;
  let fixture: ComponentFixture<LanguageSelectorComponent>;

  configureTestBed({
    declarations: [LanguageSelectorComponent],
    providers: [BsLocaleService],
    imports: [FormsModule, HttpClientTestingModule]
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
    const cookie = document.cookie.split(';').filter((item) => item.includes(`cd-lang=${lang}`));
    expect(cookie.length).toBe(1);
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

  it('should change to it-IT', () => {
    expectLanguageChange('it-IT');
  });

  it('should change to ja-JP', () => {
    expectLanguageChange('ja-JP');
  });

  it('should change to ko-KR', () => {
    expectLanguageChange('ko-KR');
  });

  it('should change to pl-PL', () => {
    expectLanguageChange('pl-PL');
  });

  it('should change to pt-BR', () => {
    expectLanguageChange('pt-BR');
  });

  it('should change to zh-CN', () => {
    expectLanguageChange('zh-CN');
  });

  it('should change to zh-TW', () => {
    expectLanguageChange('zh-TW');
  });
});
