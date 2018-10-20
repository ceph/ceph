import { Component, OnInit } from '@angular/core';
import { SupportedLanguages } from './language-selector-supported-languages.enum';

@Component({
  selector: 'cd-language-selector',
  templateUrl: './language-selector.component.html',
  styleUrls: ['./language-selector.component.scss']
})
export class LanguageSelectorComponent implements OnInit {
  localStorage = window.localStorage;
  supportedLanguages = SupportedLanguages;
  selectedLanguage: string;

  ngOnInit() {
    this.selectedLanguage = this.localStorage.getItem('lang') || 'en-US';
  }

  changeLanguage(lang: string) {
    // Set selected lang
    this.localStorage.setItem('lang', lang);

    // Reload frontend
    window.location.reload();
  }
}
