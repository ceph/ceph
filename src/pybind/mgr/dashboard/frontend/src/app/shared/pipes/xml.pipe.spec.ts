import { TestBed } from '@angular/core/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { JsonToXmlService } from '../services/json-to-xml.service';
import { XmlPipe } from './xml.pipe';

describe('XmlPipe', () => {
  let pipe: XmlPipe;
  let jsonToXmlService: JsonToXmlService;

  configureTestBed({
    providers: [JsonToXmlService]
  });

  beforeEach(() => {
    jsonToXmlService = TestBed.inject(JsonToXmlService);
    pipe = new XmlPipe(jsonToXmlService);
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });
});
