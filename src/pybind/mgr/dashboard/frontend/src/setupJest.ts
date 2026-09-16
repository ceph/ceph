import '@angular/localize/init';
import 'jest-preset-angular/setup-jest';
import './jestGlobalMocks';

import { TestBed } from '@angular/core/testing';
import { provideZoneChangeDetection } from '@angular/core';
import { TextEncoder, TextDecoder } from 'util';

Object.assign(global, { TextDecoder, TextEncoder });

process.on('unhandledRejection', (error) => {
  const stack = error['stack'] || '';
  // Avoid potential hang on test failure when running tests in parallel.
  throw `WARNING: unhandled rejection: ${error} ${stack}`;
});

beforeEach(() => {
  TestBed.configureTestingModule({
    providers: [provideZoneChangeDetection()]
  });
});
