import { ApplicationRef, enableProdMode, isDevMode } from '@angular/core';
import { enableDebugTools } from '@angular/platform-browser';
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';

import { AppModule } from './app/app.module';
import { environment } from './environments/environment';

if (environment.production) {
  enableProdMode();
}

platformBrowserDynamic()
  .bootstrapModule(AppModule)
  .then((moduleRef) => {
    if (isDevMode()) {
      const applicationRef = moduleRef.injector.get(ApplicationRef);
      const componentRef = applicationRef.components[0];
      // allows to run `ng.profiler.timeChangeDetection();`
      enableDebugTools(componentRef);
    }
  })
  .catch((err) => console.log(err));
