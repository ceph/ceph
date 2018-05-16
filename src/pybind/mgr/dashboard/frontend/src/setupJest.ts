import 'jest-preset-angular';
import 'jest-zone-patch';

// common rxjs imports
import 'rxjs/add/observable/throw';
import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/do';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/switchMap';
// ...

import './jestGlobalMocks';
