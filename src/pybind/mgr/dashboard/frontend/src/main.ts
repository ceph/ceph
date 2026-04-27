import { setRemoteDefinitions } from '@nx/angular/mf';

fetch('/assets/module-federation.manifest.json')
  .then((res) => res.json())
  .then(
    (definitions) =>
      definitions !== null && definitions !== undefined && setRemoteDefinitions(definitions)
  )
  // eslint-disable-next-line no-console
  .then(() => import('./bootstrap').catch((err) => console.error(err)));
