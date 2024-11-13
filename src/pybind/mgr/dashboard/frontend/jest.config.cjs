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
  coverageReporters: ['cobertura', 'html'],
  modulePathIgnorePatterns: [
    '<rootDir>/coverage/',
    '<rootDir>/node_modules/simplebar-angular',
    '<rootDir>/cypress'
  ],
  testMatch: ['**/*.spec.ts'],
  testRunner: 'jest-jasmine2',
  testEnvironmentOptions: {
    detectOpenHandles: true
  }
};
module.exports = jestConfig;
