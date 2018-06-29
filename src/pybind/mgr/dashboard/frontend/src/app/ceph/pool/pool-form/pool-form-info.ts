import { CrushRule } from '../../../shared/models/crush-rule';

export class PoolFormInfo {
  pool_names: string[];
  osd_count: number;
  is_all_bluestore: boolean;
  compression_algorithms: string[];
  compression_modes: string[];
  crush_rules_replicated: CrushRule[];
  crush_rules_erasure: CrushRule[];
}
