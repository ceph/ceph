import { Injectable } from '@angular/core';

/**
 * Browser-native XML parsing service.
 *
 * This replaces the Node-only `xml2js` library, which requires Node core
 * modules (`timers`, `stream`, ...) that cannot be bundled for the browser by
 * esbuild (the Angular `application` builder). It reproduces the object shape
 * produced by `xml2js` when configured with `{ explicitArray: false, trim: true }`,
 * which is all the dashboard relies on:
 *
 *   - the root element name becomes the single top-level key,
 *   - an element with only text resolves to its trimmed text value,
 *   - an element with children resolves to an object keyed by child tag name,
 *   - a tag that appears once is a value; a tag that repeats becomes an array.
 */
@Injectable({
  providedIn: 'root'
})
export class XmlService {
  /**
   * Parse an XML string into a plain object.
   *
   * @param xml the XML string to parse
   * @returns the parsed object, or `null` when the input is empty or malformed
   */
  parse(xml: string): any {
    if (!xml) {
      return null;
    }

    const doc = new DOMParser().parseFromString(xml, 'application/xml');
    if (doc.querySelector('parsererror') || !doc.documentElement) {
      return null;
    }

    return { [doc.documentElement.nodeName]: this.elementToJs(doc.documentElement) };
  }

  private elementToJs(element: Element): any {
    const children = Array.from(element.children);

    if (children.length === 0) {
      return (element.textContent ?? '').trim();
    }

    const result: Record<string, any> = {};
    for (const child of children) {
      const name = child.nodeName;
      const value = this.elementToJs(child);

      if (result[name] === undefined) {
        result[name] = value;
      } else if (Array.isArray(result[name])) {
        result[name].push(value);
      } else {
        result[name] = [result[name], value];
      }
    }
    return result;
  }
}
