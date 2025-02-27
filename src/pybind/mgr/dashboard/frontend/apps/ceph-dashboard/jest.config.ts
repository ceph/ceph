const esModules = [
  '@angular',
  '@ngrx',
  '@progress',
  'simplebar',
  'lodash-es',
  'react-syntax-highlighter',
  'swagger-client',
  '@ng-bootstrap'
];
const jestConfig = {
  moduleNameMapper: {
    '\\.scss$': 'identity-obj-proxy',
    '~/(.*)$': '<rootDir>/src/$1',
    '^@carbon/icons/es/(.*)$': '@carbon/icons/lib/$1.js'
  },
  moduleFileExtensions: ['ts', 'html', 'js', 'json', 'mjs', 'cjs'],
  preset: 'jest-preset-angular',
  setupFilesAfterEnv: ['<rootDir>/src/setupJest.ts'],
  transformIgnorePatterns: ['node_modules/(?!.*\\.mjs$|'.concat(esModules.join('|'), ')')],
  transform: {
    '^.+\\.(ts|html|mjs)$': [
      'jest-preset-angular',
      {
        useESM: true,
        stringifyContentPathRegex: '\\.(html|svg)$',
        tsconfig: '<rootDir>/tsconfig.spec.json',
        isolatedModules: true
      }
    ],
    '^.+\\.(js)$': 'babel-jest'
  },
  setupFiles: ['jest-canvas-mock'],
  modulePathIgnorePatterns: [
    '/coverage/',
    '/node_modules/simplebar-angular',
    '/cypress'
  ],
  // testMatch: ['apps/ceph-dashboard/**/*.spec.ts'],
  coverageDirectory: '../../coverage/apps/ceph-dashboard',
  testMatch: ['**/+(*.)+(spec|test).+(ts|js)?(x)'], // Ensure this matches your test file naming convention
  testRunner: 'jest-jasmine2',
  testEnvironmentOptions: {
    detectOpenHandles: true
  },
  snapshotSerializers: [
    'jest-preset-angular/build/serializers/no-ng-attributes',
    'jest-preset-angular/build/serializers/ng-snapshot',
    'jest-preset-angular/build/serializers/html-comment',
  ],
};

export default jestConfig;