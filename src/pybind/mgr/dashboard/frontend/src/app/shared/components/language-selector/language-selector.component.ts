import { Component, OnInit } from '@angular/core';

import _ from 'lodash';

import { LanguageService } from '~/app/shared/services/language.service';
import { SupportedLanguages } from './supported-languages.enum';

@Component({
  selector: 'cd-language-selector',
  templateUrl: './language-selector.component.html',
  styleUrls: ['./language-selector.component.scss']
})
export class LanguageSelectorComponent implements OnInit {
  allLanguages = SupportedLanguages;
  supportedLanguages: Record<string, any> = {};
  selectedLanguage: string;

  constructor(private languageService: LanguageService) {}

  ngOnInit() {
    this.selectedLanguage = this.languageService.getLocale();

    this.languageService.getLanguages().subscribe((langs) => {
      this.supportedLanguages = _.pick(SupportedLanguages, langs) as Object;
    });
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
