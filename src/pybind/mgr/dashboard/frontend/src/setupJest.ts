import '@angular/localize/init';

import 'jest-preset-angular/setup-jest';

import './jestGlobalMocks';

process.on('unhandledRejection', (error) => {
  const stack = error['stack'] || '';
  // Avoid potential hang on test failure when running tests in parallel.
  throw `WARNING: unhandled rejection: ${error} ${stack}`;
});
