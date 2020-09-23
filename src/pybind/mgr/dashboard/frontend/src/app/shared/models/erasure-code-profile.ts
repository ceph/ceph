export class ErasureCodeProfile {
  name: string;
  plugin: string;
  k?: number;
  m?: number;
  c?: number;
  l?: number;
  d?: number;
  packetsize?: number;
  technique?: string;
  scalar_mds?: 'jerasure' | 'isa' | 'shec';
  'crush-root'?: string;
  'crush-locality'?: string;
  'crush-failure-domain'?: string;
  'crush-device-class'?: string;
  'directory'?: string;
}
