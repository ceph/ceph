export interface MgrModuleInfo {
  name: string;
  enabled: boolean;
  always_on: boolean;
  options: Record<string, MgrModuleOption>;
}

export function decodeModuleName(value: string): string {
  if (!value) {
    return '';
  }

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
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
