import { TestBed } from '@angular/core/testing';

import { JsonToXmlService } from './json-to-xml.service';

describe('JsonToXmlService', () => {
  let service: JsonToXmlService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(JsonToXmlService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should transform JSON formatted string to XML string', () => {
    const json: string = `{
      "foo": "bar",
      "items": [
        {
          "name": "item1",
          "value": "value1"
        },
        {
          "name": "item2",
          "value": "value2"
        }
      ]
    }`;
    const expectedXml = `<foo>bar</foo>
<items>
  <name>item1</name>
  <value>value1</value>
</items>
<items>
  <name>item2</name>
  <value>value2</value>
</items>
`;
    expect(JSON.parse(json)).toBeTruthy();
    expect(service.format(json)).toBe(expectedXml);
  });
});
