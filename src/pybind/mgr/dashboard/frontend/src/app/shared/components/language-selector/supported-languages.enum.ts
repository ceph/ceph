import {
  csLocale,
  deLocale,
  esLocale,
  frLocale,
  idLocale,
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
  'fr-FR' = 'Français',
  'id-ID' = 'Bahasa Indonesia',
  'pl-PL' = 'Polski',
  'pt-PT' = 'Português',
  'es-ES' = 'Español',
  'zh-CN' = '中文'
}

// Supported languages:
// https://github.com/valor-software/ngx-bootstrap/tree/development/src/chronos/i18n
export let languageBootstrapMapping = {
  cs: csLocale,
  de: deLocale,
  es: esLocale,
  fr: frLocale,
  id: idLocale,
  pl: plLocale,
  pt: ptBrLocale,
  zh: zhCnLocale
};
