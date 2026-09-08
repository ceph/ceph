import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class RgwTopicKeyService {
  decodeTopicKey(topicKey: string): string {
    try {
      return decodeURIComponent(topicKey);
    } catch {
      return topicKey;
    }
  }

  extractTopicName(topicKey: string): string {
    const decoded = this.decodeTopicKey(topicKey);
    const segments = decoded.split(':');
    return segments[segments.length - 1] || decoded;
  }
}
