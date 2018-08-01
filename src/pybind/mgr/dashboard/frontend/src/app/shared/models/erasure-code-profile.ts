export class ErasureCodeProfile {
  name: string;
  plugin: string;
  k?: number;
  m?: number;
  c?: number;
  l?: number;
  packetsize?: number;
  technique?: string;
  'crush-root'?: string;
  'crush-locality'?: string;
  'crush-failure-domain'?: string;
  'crush-device-class'?: string;
  'directory'?: string;
}
