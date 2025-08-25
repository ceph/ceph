import { ModuleFederationConfig } from '@nx/module-federation';

const config: ModuleFederationConfig = {
  name: 'ceph-dashboard',
  remotes: [],
  shared: (libraryName, sharedConfig) => {
    if (libraryName === '@angular/localize' || libraryName === '@angular/localize/init') {
      return false; // do not share these, import directly via polyfills
    }
    return sharedConfig;
  },
};

export default config;
