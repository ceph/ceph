export default {
  displayName: 'shared',
  preset: '../../jest.preset.js',
  setupFilesAfterEnv: ['<rootDir>/src/test-setup.ts'],
  coverageDirectory: '../../coverage/libs/shared',
  transform: {
    '^.+\\.(ts|js|mjs|html)$': [
      'jest-preset-angular',
      {
        tsconfig: '<rootDir>/tsconfig.spec.json',
        stringifyContentPathRegex: '\\.(html|svg)$',
        diagnostics: false
      }
    ],
    '^.+\\.(mjs)$': 'babel-jest'
  },
transformIgnorePatterns: [
    'node_modules/(?!(carbon-components-angular|@carbon|carbon-components|@carbon/icons|jest-preset-angular|@angular|rxjs|@ng-bootstrap|ng2-charts|lodash-es|ngx-toastr)/)'
  ],
  snapshotSerializers: [  
    'jest-preset-angular/build/serializers/no-ng-attributes',
    'jest-preset-angular/build/serializers/ng-snapshot',
    'jest-preset-angular/build/serializers/html-comment'
  ]
};
