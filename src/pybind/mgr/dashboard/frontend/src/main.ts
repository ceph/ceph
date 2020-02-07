import { enableProdMode } from '@angular/core';
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';

import { AppModule } from './app/app.module';
import { environment } from './environments/environment';

if (environment.production) {
  enableProdMode();
}

platformBrowserDynamic()
  .bootstrapModule(AppModule)
  .catch((err) => console.log(err));

/*
 * Temporary fix for Chrome 80 bug regarding reduce method
 * source: https://bugs.chromium.org/p/chromium/issues/detail?id=1049982#c7
 */
(function() {
  function getChromeVersion() {
    const raw = navigator.userAgent.match(/Chrom(e|ium)\/([0-9]+)\./);
    return raw ? parseInt(raw[2], 10) : false;
  }

  const chromeVersion = getChromeVersion();
  if (chromeVersion && chromeVersion >= 80) {
    const arrayReduce = Array.prototype.reduce;
    let callback;
    Object.defineProperty(Array.prototype, 'reduce', {
      value: function(cb: any, ...args: any[]) {
        callback = cb;
        return arrayReduce.call(this, callback, ...args);
      }
    });
  }
})();
