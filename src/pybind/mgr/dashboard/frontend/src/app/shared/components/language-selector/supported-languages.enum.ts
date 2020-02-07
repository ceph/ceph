import {
  csLocale,
  deLocale,
  esLocale,
  frLocale,
  idLocale,
  itLocale,
  jaLocale,
  koLocale,
  plLocale,
  ptBrLocale,
  zhCnLocale
} from 'ngx-bootstrap/chronos';

// When adding a new supported language make sure to add a test for it in:
// language-selector.component.spec.ts
export enum SupportedLanguages {
  'cs' = 'Čeština',
  'de' = 'Deutsch',
  'en' = 'English',
  'es' = 'Español',
  'fr' = 'Français',
  'id' = 'Bahasa Indonesia',
  'it' = 'Italiano',
  'ja' = '日本語',
  'ko' = '한국어',
  'pl' = 'Polski',
  'pt' = 'Português (brasileiro)',
  'zh-Hans' = '中文 (简体)',
  'zh-Hant' = '中文 (繁體）'
}

// Supported languages:
// https://github.com/valor-software/ngx-bootstrap/tree/development/src/chronos/i18n
export let languageBootstrapMapping = {
  cs: csLocale,
  de: deLocale,
  es: esLocale,
  fr: frLocale,
  id: idLocale,
  it: itLocale,
  ja: jaLocale,
  ko: koLocale,
  pl: plLocale,
  pt: ptBrLocale,
  zh: zhCnLocale
};
