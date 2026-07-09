import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import angular from 'angular-eslint';

export default tseslint.config(
  {
    ignores: ['projects/**/*']
  },
  {
    files: ['**/*.ts'],
    extends: [
      eslint.configs.recommended,
      ...tseslint.configs.recommended,
      ...tseslint.configs.recommendedTypeChecked,
      ...angular.configs.tsRecommended
    ],
    processor: angular.processInlineTemplates,
    languageOptions: {
      parserOptions: {
        project: ['tsconfig.json'],
        createDefaultProgram: true
      }
    },
    rules: {
      'no-multiple-empty-lines': [
        'error',
        {
          max: 2,
          maxEOF: 1
        }
      ],
      'spaced-comment': [
        'error',
        'always',
        {
          exceptions: ['-', '+', '*']
        }
      ],
      curly: ['error', 'multi-line'],
      'guard-for-in': 'error',
      'no-restricted-imports': [
        'error',
        {
          paths: [
            'rxjs/Rx',
            {
              name: '@angular/core/testing',
              importNames: ['async']
            }
          ],
          patterns: ['(\\.{1,2}/){2,}']
        }
      ],
      'no-console': [
        'error',
        {
          allow: ['debug', 'info', 'time', 'timeEnd', 'trace']
        }
      ],
      'no-trailing-spaces': 'error',
      'no-caller': 'error',
      'no-bitwise': 'error',
      'no-duplicate-imports': 'error',
      'no-eval': 'error',
      '@angular-eslint/directive-selector': [
        'error',
        {
          type: 'attribute',
          prefix: 'cd',
          style: 'camelCase'
        }
      ],
      '@angular-eslint/component-selector': [
        'error',
        {
          type: 'element',
          prefix: 'cd',
          style: 'kebab-case'
        }
      ]
    }
  },
  {
    files: ['**/*.html'],
    extends: [
      ...angular.configs.templateRecommended
    ],
    rules: {
      '@angular-eslint/template/eqeqeq': 'off',
      '@angular-eslint/template/alt-text': 'error',
      '@angular-eslint/template/no-duplicate-attributes': 'error',
      '@angular-eslint/template/no-distracting-elements': 'error',
      '@angular-eslint/template/valid-aria': 'error',
      '@angular-eslint/template/table-scope': 'error',
      '@angular-eslint/template/no-positive-tabindex': 'error',
      '@angular-eslint/template/button-has-type': 'error',
      '@angular-eslint/template/interactive-supports-focus': 'error',
      '@angular-eslint/template/click-events-have-key-events': 'error',
      '@angular-eslint/template/label-has-associated-control': 'error',
      '@angular-eslint/template/elements-content': 'error'
    }
  }
);
