export interface MgrModuleInfo {
  name: string;
  enabled: boolean;
  always_on: boolean;
  options: Record<string, MgrModuleOption>;
}

interface MgrModuleOption {
  name: string;
  type: string;
  level: string;
  flags: number;
  default_value: number;
  min: string;
  max: string;
  enum_allowed: string[];
  desc: string;
  long_desc: string;
  tags: string[];
  see_also: string[];
}
