import { CrushStep } from './crush-step';

export class CrushRule {
  max_size: number;
  min_size: number;
  rule_id: number;
  rule_name: string;
  ruleset: number;
  steps: CrushStep[];
}
