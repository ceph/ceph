import { Pipe, PipeTransform } from '@angular/core';
import { JsonToXmlService } from '../services/json-to-xml.service';

@Pipe({
  name: 'xml'
})
export class XmlPipe implements PipeTransform {
  constructor(private jsonToXmlService: JsonToXmlService) {}

  transform(value: string, valueFormat: string = 'json'): string {
    if (valueFormat === 'json') {
      value = this.jsonToXmlService.format(value);
    }
    return value;
  }
}
