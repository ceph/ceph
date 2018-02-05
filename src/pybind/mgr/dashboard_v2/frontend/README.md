# Ceph Dashboard

This project was generated with [Angular CLI](https://github.com/angular/angular-cli) version 1.6.3.

## Installation

Run `npm install` to install the required packages locally.

**Note**

If you do not have installed [Angular CLI](https://github.com/angular/angular-cli) globally, then you need to execute ``ng`` commands with an additional ``npm run`` before it.

## Development server

Create the `proxy.conf.json` file based on `proxy.conf.json.sample`.

Run `npm start -- --proxy-config proxy.conf.json` for a dev server. 
Navigate to `http://localhost:4200/`. 
The app will automatically reload if you change any of the source files.

## Code scaffolding

Run `ng generate component component-name` to generate a new component. You can also use `ng generate directive|pipe|service|class|guard|interface|enum|module`.

## Build

Run `npm run build` to build the project. The build artifacts will be stored in the `dist/` directory. Use the `-prod` flag for a production build. Navigate to `http://localhost:8080`.

## Running unit tests

Run `npm run test` to execute the unit tests via [Karma](https://karma-runner.github.io).

## Running end-to-end tests

Run `npm run e2e` to execute the end-to-end tests via [Protractor](http://www.protractortest.org/).

## Further help

To get more help on the Angular CLI use `ng help` or go check out the [Angular CLI README](https://github.com/angular/angular-cli/blob/master/README.md).

## Examples of generator

```
# Create module 'Core'
src/app> ng generate module core -m=app --routing

# Create module 'Auth' under module 'Core'
src/app/core> ng generate module auth -m=core --routing
or, alternatively:
src/app> ng generate module core/auth -m=core --routing

# Create component 'Login' under module 'Auth'
src/app/core/auth> ng generate component login -m=core/auth
or, alternatively:
src/app> ng generate component core/auth/login -m=core/auth
```

## Recommended style guide

### Typescript

Group the imports based on its source and separate them with a blank line.

The source groups can be either from angular, external or internal.

Example:
```javascript
import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { ToastsManager } from 'ng2-toastr';

import { Credentials } from '../../../shared/models/credentials.model';
import { HostService } from './services/host.service';
```