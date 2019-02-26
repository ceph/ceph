var replace = require('replace-in-file');
var buildDate = new Date();
var copyrightYear = buildDate.getFullYear();
const optionsNewYear = {
    files:[
      'src/environments/environment.ts',
      'src/environments/environment.prod.ts'
    ],
    from: /{COPYRIGHT_YEAR}/g,
    to: copyrightYear,
    allowEmptyPaths: false,
};

const optionsOldYear = {
    files:[
      'src/environments/environment.ts',
      'src/environments/environment.prod.ts'
    ],
    from: /year: '(.*)'/g,
    to: "year: '{COPYRIGHT_YEAR}'",
    allowEmptyPaths: false,
};

const optionsNewProd = {
    files:[
      'src/environments/environment.prod.ts'
    ],
    from: /'{PRODUCTION}'/g,
    to: true,
    allowEmptyPaths: false,
};

const optionsNewDev = {
    files:[
      'src/environments/environment.ts'
    ],
    from: /'{PRODUCTION}'/g,
    to: false,
    allowEmptyPaths: false,
};

const optionsOldProd = {
    files:[
      'src/environments/environment.prod.ts',
      'src/environments/environment.ts'
    ],
    from: /production: (.*)/g,
    to: "production: '{PRODUCTION}',",
    allowEmptyPaths: false,
};

try {
    let changeOldYearFiles = replace.sync(optionsOldYear);
    let changeNewYearFiles = replace.sync(optionsNewYear);
    let changeOldProdFiles = replace.sync(optionsOldProd);
    let changeProdFiles = replace.sync(optionsNewProd);
    let changeDevFiles = replace.sync(optionsNewDev);
    console.log('Environement variables has been set');
}
catch (error) {
    console.error('Error occurred:', error);
    throw error
}
