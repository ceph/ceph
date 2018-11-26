import { CrushRule } from './crush-rule';

export class PoolFormInfo {
  pool_names: string[];
  osd_count: number;
  is_all_bluestore: boolean;
  bluestore_compression_algorithm: string;
  compression_algorithms: string[];
  compression_modes: string[];
  crush_rules_replicated: CrushRule[];
  crush_rules_erasure: CrushRule[];
}
