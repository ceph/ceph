import { enableProdMode, MissingTranslationStrategy } from '@angular/core';
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';

import { AppModule } from './app/app.module';
import { i18nProviders } from './app/locale.helper';
import { environment } from './environments/environment';

if (environment.production) {
  enableProdMode();
}

platformBrowserDynamic()
  .bootstrapModule(AppModule, {
    missingTranslation: MissingTranslationStrategy.Ignore,
    providers: i18nProviders
  })
  .catch((err) => console.log(err));
