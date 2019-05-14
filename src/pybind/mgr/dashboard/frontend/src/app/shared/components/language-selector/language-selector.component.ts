import { Component, Input, OnInit } from '@angular/core';

import { defineLocale } from 'ngx-bootstrap/chronos';
import { BsLocaleService } from 'ngx-bootstrap/datepicker';

import { LocaleHelper } from '../../../locale.helper';
import { languageBootstrapMapping, SupportedLanguages } from './supported-languages.enum';

@Component({
  selector: 'cd-language-selector',
  templateUrl: './language-selector.component.html',
  styleUrls: ['./language-selector.component.scss']
})
export class LanguageSelectorComponent implements OnInit {
  @Input()
  isDropdown = true;

  supportedLanguages = SupportedLanguages;
  selectedLanguage: string;

  constructor(private localeService: BsLocaleService) {}

  ngOnInit() {
    this.selectedLanguage = LocaleHelper.getLocale();
    this.defineUsedLanguage();
  }

  /**
   * Sets ngx-bootstrap local based on the current language selection
   *
   * ngx-bootstrap locals documentation:
   * https://valor-software.com/ngx-bootstrap/#/datepicker#locales
   */
  private defineUsedLanguage() {
    const lang = this.selectedLanguage.slice(0, 2);
    if (lang in languageBootstrapMapping) {
      defineLocale(lang, languageBootstrapMapping[lang]);
      this.localeService.use(lang);
    }
  }

  changeLanguage(lang: string) {
    LocaleHelper.setLocale(lang);
    window.location.reload();
  }
}
