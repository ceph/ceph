import { enableProdMode } from '@angular/core';
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';

import { AppModule } from './app/app.module';
import { environment } from './environments/environment';

import { getTranslationProviders } from './app/providers/i18n.provider';

if (environment.production) {
  enableProdMode();
}

const locale = window.localStorage.getItem('lang');

getTranslationProviders(locale).then(providers => {
  platformBrowserDynamic().bootstrapModule(AppModule, {providers})
    .catch(err => console.log(err));
});
