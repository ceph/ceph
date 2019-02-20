import localeDe from '@angular/common/locales/de';
import localeEn from '@angular/common/locales/en';
import localePt from '@angular/common/locales/pt';
import { LOCALE_ID, TRANSLATIONS, TRANSLATIONS_FORMAT } from '@angular/core';
import { defineLocale, deLocale, ptBrLocale } from 'ngx-bootstrap/chronos';

declare const require;

export class LocaleHelper {
  constructor() {
    defineLocale('de', deLocale);
    defineLocale('pt', ptBrLocale);
  }

  static getBrowserLang(): string {
    const lang = navigator.language;

    if (lang.includes('en')) {
      return 'en-US';
    } else if (lang.includes('pt')) {
      return 'pt-PT';
    } else if (lang.includes('de')) {
      return 'de-DE';
    } else {
      return undefined;
    }
  }

  static getLocale(): string {
    return window.localStorage.getItem('lang') || this.getBrowserLang() || 'en-US';
  }

  static setLocale(lang: string) {
    window.localStorage.setItem('lang', lang);
  }

  static getLocaleData() {
    let localeData = localeEn;
    switch (this.getLocale()) {
      case 'pt-PT':
        localeData = localePt;
        break;
      case 'de-DE':
        localeData = localeDe;
        break;
    }
    return localeData;
  }
}

const i18nProviders = [
  { provide: LOCALE_ID, useValue: LocaleHelper.getLocale() },
  {
    provide: TRANSLATIONS,
    useFactory: (locale) => {
      locale = locale || 'en-US';
      try {
        return require(`raw-loader!locale/messages.${locale}.xlf`);
      } catch (error) {
        return [];
      }
    },
    deps: [LOCALE_ID]
  },
  { provide: TRANSLATIONS_FORMAT, useValue: 'xlf' }
];

export { i18nProviders };
