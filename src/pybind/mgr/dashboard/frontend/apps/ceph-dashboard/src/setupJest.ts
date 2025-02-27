import '@angular/localize/init';

import 'jest-preset-angular/setup-jest';

import './jestGlobalMocks';

import { TextEncoder, TextDecoder } from 'util';

Object.assign(global, { TextDecoder, TextEncoder });

process.on('unhandledRejection', (error: Record<string, unknown>) => {
  const stack = error['stack'] || '';
  // Avoid potential hang on test failure when running tests in parallel.
  throw `WARNING: unhandled rejection: ${error} ${stack}`;
});
