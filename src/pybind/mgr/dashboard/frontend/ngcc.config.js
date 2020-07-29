module.exports = {
  packages: {
    'simplebar-angular': {
      ignorableDeepImportMatchers: [/simplebar-core\.esm/]
    },
    '@locl/cli': {
      ignorableDeepImportMatchers: [/@angular\/localize/]
    }
  }
};
