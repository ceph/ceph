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
export function cdEncode(...args: any[]): any {
  switch (args.length) {
    case 1:
      return encodeClass.apply(this, args);
    case 3:
      return encodeMethod.apply(this, args);
    default:
      throw new Error();
  }
}

/**
 * This decorator can be used in parameters only.
 * It will exclude the parameter from being encode.
 * This should be used in parameters that are going
 * to be sent in the request's body.
 *
 * @export
 * @param {Object} target
 * @param {string} propertyKey
 * @param {number} index
 */
export function cdEncodeNot(target: Object, propertyKey: string, index: number) {
  const metadataKey = `__ignore_${propertyKey}`;
  if (Array.isArray(target[metadataKey])) {
    target[metadataKey].push(index);
  } else {
    target[metadataKey] = [index];
  }
}

function encodeClass(target: Function) {
  for (const propertyName of Object.keys(target.prototype)) {
    const descriptor = Object.getOwnPropertyDescriptor(target.prototype, propertyName);

    const isMethod = descriptor.value instanceof Function;
    if (!isMethod) {
      continue;
    }

    encodeMethod(target.prototype, propertyName, descriptor);
    Object.defineProperty(target.prototype, propertyName, descriptor);
  }
}

function encodeMethod(target: any, propertyKey: string, descriptor: PropertyDescriptor) {
  if (descriptor === undefined) {
    descriptor = Object.getOwnPropertyDescriptor(target, propertyKey);
  }
  const originalMethod = descriptor.value;

  descriptor.value = function() {
    const metadataKey = `__ignore_${propertyKey}`;
    const indices: number[] = target[metadataKey] || [];
    const args = [];

    for (let i = 0; i < arguments.length; i++) {
      if (_.isString(arguments[i]) && indices.indexOf(i) === -1) {
        args[i] = encodeURIComponent(arguments[i]);
      } else {
        args[i] = arguments[i];
      }
    }

    const result = originalMethod.apply(this, args);
    return result;
  };
}
