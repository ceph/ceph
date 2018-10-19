import { Component, Input, OnInit } from '@angular/core';

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

  ngOnInit() {
    this.selectedLanguage = LocaleHelper.getLocale();
  }

  changeLanguage(lang: string) {
    LocaleHelper.setLocale(lang);

    // Reload frontend
    window.location.reload();
  }
}
