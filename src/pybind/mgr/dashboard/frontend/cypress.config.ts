import { defineConfig } from 'cypress';
import fs from 'fs'

export default defineConfig({
  video: true,
  videoUploadOnPasses: false,
  defaultCommandTimeout: 120000,
  responseTimeout: 45000,
  viewportHeight: 1080,
  viewportWidth: 1920,
  projectId: 'k7ab29',
  reporter: 'cypress-multi-reporters',

  reporterOptions: {
    reporterEnabled: 'spec, mocha-junit-reporter',
    mochaJunitReporterReporterOptions: {
      mochaFile: 'cypress/reports/results-[hash].xml'
    }
  },

  retries: 1,

  env: {
    LOGIN_USER: 'admin',
    LOGIN_PWD: 'admin',
    CEPH2_URL: 'https://localhost:4202/'
  },

  chromeWebSecurity: false,
  eyesIsDisabled: false,
  eyesFailCypressOnDiff: true,
  eyesDisableBrowserFetching: false,
  eyesLegacyHooks: true,
  eyesTestConcurrency: 5,
  eyesPort: 35321,

  e2e: {
    // We've imported your old cypress plugins here.
    // You may want to clean this up later by importing these.
    setupNodeEvents(on, config) {
      on(
        'after:spec',
        (_: Cypress.Spec, results: CypressCommandLine.RunResult) => {
          if (results && results.video) {
            // Do we have failures for any retry attempts?
            const failures = results.tests.some((test) =>
              test.attempts.some((attempt) => attempt.state === 'failed')
            )
            if (!failures) {
              // delete the video if the spec passed and no tests retried
              fs.unlinkSync(results.video)
            }
          }
        }
      )
      return require('./cypress/plugins/index.js')(on, config);
    },
    baseUrl: 'https://localhost:4200/',
    excludeSpecPattern: ['*.po.ts', '**/orchestrator/**'],
    experimentalSessionAndOrigin: true,
    specPattern: 'cypress/e2e/**/*-spec.{js,jsx,ts,tsx,feature}'
  },

  component: {
    devServer: {
      framework: 'angular',
      bundler: 'webpack'
    },
    specPattern: '**/*.cy.ts'
  }
});
