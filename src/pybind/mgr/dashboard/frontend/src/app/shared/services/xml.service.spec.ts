import { TestBed } from '@angular/core/testing';

import { XmlService } from './xml.service';

describe('XmlService', () => {
  let service: XmlService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(XmlService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('parse', () => {
    it('should return null for an empty string', () => {
      expect(service.parse('')).toBeNull();
    });

    it('should return null for a null input', () => {
      expect(service.parse(null)).toBeNull();
    });

    it('should return null for malformed XML', () => {
      expect(service.parse('<Root><unclosed></Root>')).toBeNull();
    });

    it('should use the root element name as the single top-level key', () => {
      expect(service.parse('<Root>value</Root>')).toEqual({ Root: 'value' });
    });

    it('should resolve a leaf element to its trimmed text value', () => {
      expect(service.parse('<Root>  spaced  </Root>')).toEqual({ Root: 'spaced' });
    });

    it('should resolve an empty element to an empty string', () => {
      expect(service.parse('<Root></Root>')).toEqual({ Root: '' });
    });

    it('should resolve an element with children to an object keyed by tag name', () => {
      const result = service.parse('<Root><Key>k</Key><Value>v</Value></Root>');
      expect(result).toEqual({ Root: { Key: 'k', Value: 'v' } });
    });

    it('should keep a single occurrence of a tag as a value', () => {
      const result = service.parse('<Root><Item>a</Item></Root>');
      expect(result).toEqual({ Root: { Item: 'a' } });
    });

    it('should collect repeated tags into an array', () => {
      const result = service.parse('<Root><Item>a</Item><Item>b</Item><Item>c</Item></Root>');
      expect(result).toEqual({ Root: { Item: ['a', 'b', 'c'] } });
    });

    it('should parse a nested S3 ACL document with a single grant', () => {
      const xml = `
        <AccessControlPolicy>
          <Owner><ID>owner-id</ID></Owner>
          <AccessControlList>
            <Grant>
              <Grantee><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee>
              <Permission>READ</Permission>
            </Grant>
          </AccessControlList>
        </AccessControlPolicy>`;
      const grant = service.parse(xml)['AccessControlPolicy']['AccessControlList']['Grant'];
      expect(Array.isArray(grant)).toBe(false);
      expect(grant).toEqual({
        Grantee: { URI: 'http://acs.amazonaws.com/groups/global/AllUsers' },
        Permission: 'READ'
      });
    });

    it('should parse a nested S3 ACL document with multiple grants as an array', () => {
      const xml = `
        <AccessControlPolicy>
          <AccessControlList>
            <Grant>
              <Grantee><ID>owner-id</ID></Grantee>
              <Permission>FULL_CONTROL</Permission>
            </Grant>
            <Grant>
              <Grantee><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee>
              <Permission>READ</Permission>
            </Grant>
          </AccessControlList>
        </AccessControlPolicy>`;
      const grants = service.parse(xml)['AccessControlPolicy']['AccessControlList']['Grant'];
      expect(Array.isArray(grants)).toBe(true);
      expect(grants.length).toBe(2);
      expect(grants[1]['Grantee']['URI']).toContain('AllUsers');
      expect(grants[1]['Permission']).toBe('READ');
    });
  });
});
