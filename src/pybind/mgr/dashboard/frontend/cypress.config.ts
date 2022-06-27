import { defineConfig } from "cypress";

export default defineConfig({
  video: false,
  defaultCommandTimeout: 120000,
  responseTimeout: 45000,
  viewportHeight: 1080,
  viewportWidth: 1920,
  projectId: "k7ab29",
  reporter: "cypress-multi-reporters",

  reporterOptions: {
    reporterEnabled: "spec, mocha-junit-reporter",
    mochaJunitReporterReporterOptions: {
      mochaFile: "cypress/reports/results-[hash].xml",
    },
  },

  retries: 1,

  env: {
    LOGIN_USER: "admin",
    LOGIN_PWD: "admin",
  },

  e2e: {
    // We've imported your old cypress plugins here.
    // You may want to clean this up later by importing these.
    setupNodeEvents(on, config) {
      return require("./cypress/plugins/index.js")(on, config);
    },
    baseUrl: "https://127.0.0.1:4200/",
    excludeSpecPattern: ["*.po.ts", "**/orchestrator/**"],
    specPattern: "cypress/e2e/**/*.e2e-spec.ts",
  },
});
