import * as _ from 'lodash';

/**
 * This decorator can be used in a class or method.
 * It will encode all the string parameters of all the methods of a class
 * or, if applied on a method, the specified method.
 *
 * @export
 * @param {Function} [target=null]
 * @returns {*}
 */
export function cdEncode(target: Function = null): any {
  if (target) {
    encodeClass(target);
  } else {
    return encodeMethod();
  }
}

function encodeClass(target: Function) {
  for (const propertyName of Object.keys(target.prototype)) {
    const descriptor = Object.getOwnPropertyDescriptor(target.prototype, propertyName);
    const isMethod = descriptor.value instanceof Function;
    if (!isMethod) {
      continue;
    }

    const originalMethod = descriptor.value;
    descriptor.value = function(...args: any[]) {
      args.forEach((arg, i, argsArray) => {
        if (_.isString(arg)) {
          argsArray[i] = encodeURIComponent(arg);
        }
      });

      const result = originalMethod.apply(this, args);
      return result;
    };

    Object.defineProperty(target.prototype, propertyName, descriptor);
  }
}

function encodeMethod() {
  return function(target: any, propertyKey: string, descriptor: PropertyDescriptor) {
    if (descriptor === undefined) {
      descriptor = Object.getOwnPropertyDescriptor(target, propertyKey);
    }
    const originalMethod = descriptor.value;

    descriptor.value = function() {
      const args = [];

      for (let i = 0; i < arguments.length; i++) {
        if (_.isString(arguments[i])) {
          args[i] = encodeURIComponent(arguments[i]);
        } else {
          args[i] = arguments[i];
        }
      }

      const result = originalMethod.apply(this, args);
      return result;
    };
  };
}
