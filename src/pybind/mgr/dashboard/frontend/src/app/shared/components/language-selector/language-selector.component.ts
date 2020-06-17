import { Component, Input, OnInit } from '@angular/core';

import * as _ from 'lodash';
import { defineLocale } from 'ngx-bootstrap/chronos';
import { BsLocaleService } from 'ngx-bootstrap/datepicker';

import { LanguageService } from '../../services/language.service';
import { languageBootstrapMapping, SupportedLanguages } from './supported-languages.enum';

@Component({
  selector: 'cd-language-selector',
  templateUrl: './language-selector.component.html',
  styleUrls: ['./language-selector.component.scss']
})
export class LanguageSelectorComponent implements OnInit {
  @Input()
  isDropdown = true;

  supportedLanguages: Record<string, any> = {};
  selectedLanguage: string;

  constructor(private localeService: BsLocaleService, private languageService: LanguageService) {}

  ngOnInit() {
    this.selectedLanguage = this.languageService.getLocale();

    this.defineUsedLanguage();

    this.languageService.getLanguages().subscribe((langs) => {
      this.supportedLanguages = _.pick(SupportedLanguages, langs) as Object;
    });
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

  /**
   * Jest is being more restricted regarding spying on the reload method.
   * This will allow us to spyOn this method instead.
   */
  reloadWindow() {
    window.location.reload();
  }

  changeLanguage(lang: string) {
    this.languageService.setLocale(lang);
    this.reloadWindow();
  }
}
