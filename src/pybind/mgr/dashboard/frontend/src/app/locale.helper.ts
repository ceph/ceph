import locale_cs from '@angular/common/locales/cs';
import locale_de from '@angular/common/locales/de';
import locale_en from '@angular/common/locales/en';
import locale_es from '@angular/common/locales/es';
import locale_fr from '@angular/common/locales/fr';
import locale_id from '@angular/common/locales/id';
import locale_pl from '@angular/common/locales/pl';
import locale_pt_PT from '@angular/common/locales/pt-PT';
import locale_zh_Hans from '@angular/common/locales/zh-Hans';
import { LOCALE_ID, TRANSLATIONS, TRANSLATIONS_FORMAT } from '@angular/core';

declare const require;

export class LocaleHelper {
  static getBrowserLang(): string {
    const lang = navigator.language;

    if (lang.includes('cs')) {
      return 'cs';
    } else if (lang.includes('de')) {
      return 'de-DE';
    } else if (lang.includes('en')) {
      return 'en-US';
    } else if (lang.includes('es')) {
      return 'es-ES';
    } else if (lang.includes('fr')) {
      return 'fr-FR';
    } else if (lang.includes('id')) {
      return 'id-ID';
    } else if (lang.includes('pl')) {
      return 'pl-PL';
    } else if (lang.includes('pt')) {
      return 'pt-PT';
    } else if (lang.includes('zh')) {
      return 'zh-CN';
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
    let localeData = locale_en;
    switch (this.getLocale()) {
      case 'cs':
        localeData = locale_cs;
        break;
      case 'de-DE':
        localeData = locale_de;
        break;
      case 'es-ES':
        localeData = locale_es;
        break;
      case 'fr-FR':
        localeData = locale_fr;
        break;
      case 'id-ID':
        localeData = locale_id;
        break;
      case 'pt-PT':
        localeData = locale_pt_PT;
        break;
      case 'pl-PL':
        localeData = locale_pl;
        break;
      case 'zh-CN':
        localeData = locale_zh_Hans;
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
