/**
 * Ceph Dashboard node script
 * This file should be used to aggregate all external scripts we need.
 * Multiple flags can be used in the same call.
 *
 * Available flags:
 * --env: Generates angular environment files.
 *
 * --pre: Modifies 'angular.json' to enable the build of custom locales using
 *        angular --localize.
 *        Languages can be defined using the environment variable DASHBOARD_FRONTEND_LANGS,
 *        if no value is provided all languages will be build.
 *        Default language is always build, even if not provided.
 *        p.e.: 'DASHBOARD_FRONTEND_LANGS="pt" node cd --pre', will build EN and PT.
 *        For backward compatibility we accept both long and short version of
 *        languages, p.e.: 'pt' and 'pt-BR'
 *
 * --res: Restores 'angular.json' to its original and removes the backup file.
 */

const fs = require('fs');

const filename = './angular.json';
const backup = './angular.backup.json';

if (process.argv.includes('--env')) {
  envBuild();
}

if (process.argv.includes('--pre')) {
  prepareLocales();
}

if (process.argv.includes('--res')) {
  restoreLocales();
}

function prepareLocales() {
  try {
    fs.accessSync(backup, fs.constants.F_OK);
    logger(`'${backup}' already exists, restoring it into '${filename}'}`);
    fs.copyFileSync(backup, filename);
  } catch (err) {
    fs.copyFileSync(filename, backup);
    logger(`'${filename}' was copied to '${backup}'`);
  }

  let langs = process.env.DASHBOARD_FRONTEND_LANGS || '';
  langs = langs.replace(/\"\'/g, '')
  if (langs == 'ALL') {
    logger(`Preparing build of all languages.`);
    return;
  } else if (langs.length == 0) {
    langs = [];
    logger(`Preparing build of EN.`);
  } else {
    langs = langs.split(/[ ,]/);
    logger(`Preparing build of EN and ${langs}.`);
  }

  let angular = require(filename);

  let allLocales = angular['projects']['ceph-dashboard']['i18n']['locales'];
  let locales = {};

  langs.forEach((lang) => {
    const short = lang.substring(0, 2);
    locale = allLocales[short];
    if (locale) {
      locales[short] = locale;
    } else {
      switch (lang) {
        case 'zh-Hans':
        case 'zh-CN':
          locales['zh-Hans'] = allLocales['zh-Hans'];
          break;

        case 'zh-TW':
        case 'zh-Hant':
          locales['zh-Hant'] = allLocales['zh-Hant'];
          break;
      }
    }
  });

  angular['projects']['ceph-dashboard']['i18n']['locales'] = locales;
  const newAngular = JSON.stringify(angular, null, 2);

  fs.writeFile(filename, newAngular, (err) => {
    if (err) throw err;
    logger(`Writing to ${filename}`);
  });
}

function restoreLocales() {
  fs.access(backup, fs.constants.F_OK, (err) => {
    logger(`'${backup}' ${err ? 'does not exist' : 'exists'}`);

    if (!err) {
      fs.copyFile(backup, filename, (err) => {
        if (err) throw err;
        logger(`'${backup}' was copied to '${filename}'`);

        fs.unlink(backup, (err) => {
          if (err) throw err;
          logger(`successfully deleted '${backup}'`);
        });
      });
    }
  });
}

function envBuild() {
  origFile = 'src/environments/environment.tpl.ts';
  devFile = 'src/environments/environment.ts';
  prodFile = 'src/environments/environment.prod.ts';

  const replacements = [
    { from: '{DEFAULT_LANG}', to: process.env.npm_package_config_locale },
    { from: '{COPYRIGHT_YEAR}', to: new Date().getFullYear() }
  ];
  let dev = replacements.concat([{ from: `'{PRODUCTION}'`, to: false }]);
  let prod = replacements.concat([{ from: `'{PRODUCTION}'`, to: true }]);

  fs.copyFile(origFile, devFile, (err) => {
    if (err) throw err;
    logger(`'${origFile}' was copied to '${devFile}'`);

    replace(devFile, dev);
  });

  fs.copyFile(origFile, prodFile, (err) => {
    if (err) throw err;
    logger(`'${origFile}' was copied to '${prodFile}'`);

    replace(prodFile, prod);
  });
}

/**
 * Replace strings in a file.
 *
 * @param {*} filename Relative path to the file
 * @param {*} replacements List of replacements, each should have from' and 'to'
 * proprieties.
 */
function replace(filename, replacements) {
  fs.readFile(filename, 'utf8', (err, data) => {
    if (err) throw err;

    replacements.forEach((rep) => {
      data = data.replace(rep.from, rep.to);
    });

    fs.writeFile(filename, data, 'utf8', (err) => {
      if (err) throw err;
      logger(`Placeholders were replace in '${filename}'`);
    });
  });
}

/**
 * Writes logs to the console using the [cd.js] prefix
 */
function logger(message) {
  console.log(`[cd.js] ${message}`);
}
