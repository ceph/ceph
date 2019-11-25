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
  'de-DE' = 'Deutsch',
  'en-US' = 'English',
  'es-ES' = 'Español',
  'fr-FR' = 'Français',
  'id-ID' = 'Bahasa Indonesia',
  'it-IT' = 'Italiano',
  'ja-JP' = '日本語',
  'ko-KR' = '한국어',
  'pl-PL' = 'Polski',
  'pt-BR' = 'Português (brasileiro)',
  'zh-CN' = '中文 (简体)',
  'zh-TW' = '中文 (繁體）'
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
