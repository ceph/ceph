import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class JsonToXmlService {
  constructor() {}

  format(
    json: any,
    replaceKey: Record<string, string> = null,
    indentSize: number = 2,
    currentIndent: number = 0
  ): string {
    if (!json) return null;
    let xml = '';
    if (typeof json === 'string') {
      json = JSON.parse(json);
    }

    for (let key in json) {
      if (json.hasOwnProperty(key)) {
        const value = json[key];
        const indentation = ' '.repeat(currentIndent);
        if (replaceKey) {
          const [oldKey, newKey] = Object.entries(replaceKey)[0];
          if (key === oldKey) {
            key = newKey;
          }
        }
        if (Array.isArray(value)) {
          value.forEach((item) => {
            xml +=
              `${indentation}<${key}>\n` +
              this.format(item, replaceKey, indentSize, currentIndent + indentSize) +
              `${indentation}</${key}>\n`;
          });
        } else if (typeof value === 'object') {
          xml +=
            `${indentation}<${key}>\n` +
            this.format(value, replaceKey, indentSize, currentIndent + indentSize) +
            `${indentation}</${key}>\n`;
        } else {
          xml += `${indentation}<${key}>${value}</${key}>\n`;
        }
      }
    }
    return xml;
  }
}
