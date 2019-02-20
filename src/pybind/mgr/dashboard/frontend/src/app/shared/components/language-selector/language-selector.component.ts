import { Component, Input, OnInit } from '@angular/core';

import { defineLocale, deLocale, ptBrLocale } from 'ngx-bootstrap/chronos';

import { LocaleHelper } from '../../../locale.helper';
import { SupportedLanguages } from './supported-languages.enum';

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

  constructor() {
    // These locals are needed for ngx related localizations
    defineLocale('pt', ptBrLocale);
    defineLocale('de', deLocale);
  }

  ngOnInit() {
    this.selectedLanguage = LocaleHelper.getLocale();
  }

  changeLanguage(lang: string) {
    LocaleHelper.setLocale(lang);

    // Reload frontend
    window.location.reload();
  }
}
