import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwTopicKeyService } from './rgw-topic-key.service';

describe('RgwTopicKeyService', () => {
  let service: RgwTopicKeyService;

  configureTestBed({
    providers: [RgwTopicKeyService]
  });

  beforeEach(() => {
    service = TestBed.inject(RgwTopicKeyService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('decodeTopicKey', () => {
    it('should decode a valid URL-encoded topic key', () => {
      const input = 'arn%3Aaws%3Asns%3Aus-east-1%3A12345%3Amy-topic';
      const expected = 'arn:aws:sns:us-east-1:12345:my-topic';
      expect(service.decodeTopicKey(input)).toBe(expected);
    });

    it('should return the original string if no encoding is present', () => {
      const input = 'arn:aws:sns:us-east-1:12345:my-topic';
      expect(service.decodeTopicKey(input)).toBe(input);
    });

    it('should catch errors and return original string for a malformed URI', () => {
      // '%1' is a malformed URI component that triggers decodeURIComponent to throw
      const malformedInput = 'arn%3Aaws%3Asns%3A%1';
      expect(service.decodeTopicKey(malformedInput)).toBe(malformedInput);
    });
  });

  describe('extractTopicName', () => {
    it('should extract the last segment of a decoded topic key', () => {
      const input = 'arn:aws:sns:us-east-1:12345:my-topic';
      expect(service.extractTopicName(input)).toBe('my-topic');
    });

    it('should extract the last segment of an encoded topic key', () => {
      const input = 'arn%3Aaws%3Asns%3Aus-east-1%3A12345%3Amy-topic';
      expect(service.extractTopicName(input)).toBe('my-topic');
    });

    it('should return the whole string if there are no colons', () => {
      const input = 'my-topic-only';
      expect(service.extractTopicName(input)).toBe('my-topic-only');
    });

    it('should return the full decoded string if the last segment is empty', () => {
      // Since split(':') creates an empty string at the end,
      // the || operator in your service falls back to the full decoded string.
      const input = 'arn:aws:sns:us-east-1:12345:';
      expect(service.extractTopicName(input)).toBe(input);
    });
  });
});
