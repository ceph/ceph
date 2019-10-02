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
    } else if (lang.includes('it')) {
      return 'it-IT';
    } else if (lang.includes('ja')) {
      return 'ja-JP';
    } else if (lang.includes('pl')) {
      return 'pl-PL';
    } else if (lang.includes('pt')) {
      return 'pt-BR';
    } else if (lang.includes('zh-TW')) {
      return 'zh-TW';
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
    document.cookie = `cd-lang=${lang}`;
    window.localStorage.setItem('lang', lang);
  }
}

