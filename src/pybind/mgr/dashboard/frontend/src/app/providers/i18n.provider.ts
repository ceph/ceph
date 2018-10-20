import { TRANSLATIONS, TRANSLATIONS_FORMAT } from '@angular/core';
declare const require;

export function getTranslationProviders(locale: string): Promise<any[]> {
  const localeValue = locale || 'en-US' as string;
  const noProviders: Object[] = [];

  // If no locale is set or it's en-US, then doesn't require translation
  if ( !localeValue || localeValue === 'en-US' ) {
    return Promise.resolve( noProviders );
  }

  try {
    const translations = require(`raw-loader!../../locale/${ localeValue }/messages.xlf`);
    return Promise.resolve([
      { provide: TRANSLATIONS, useValue: translations },
      { provide: TRANSLATIONS_FORMAT, useValue: 'xlf' }
    ]);
  } catch (error) {
    return Promise.resolve(noProviders);
  }
}
