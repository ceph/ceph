import { CrushStep } from './crush-step';

export class CrushRule {
  usable_size?: number;
  rule_id: number;
  type: number;
  rule_name: string;
  steps: CrushStep[];
}

export class CrushRuleConfig {
  root: string; // The name of the node under which data should be placed.
  name: string;
  failure_domain: string; // The type of CRUSH nodes across which we should separate replicas.
  device_class?: string; // The device class data should be placed on.
}
