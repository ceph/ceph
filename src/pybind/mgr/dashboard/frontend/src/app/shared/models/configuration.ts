export enum RbdConfigurationSourceField {
  global = 0,
  pool = 1,
  image = 2
}

export enum RbdConfigurationType {
  bps,
  iops,
  milliseconds
}

/**
 * This configuration can also be set on a pool level.
 */
export interface RbdConfigurationEntry {
  name: string;
  source: RbdConfigurationSourceField;
  value: any;
  type?: RbdConfigurationType; // Non-external field.
  description?: string; // Non-external field.
  displayName?: string; // Non-external field. Nice name for the UI which is added in the UI.
}

/**
 * This object contains additional information injected into the elements retrieved by the service.
 */
export interface RbdConfigurationExtraField {
  name: string;
  displayName: string;
  description: string;
  type: RbdConfigurationType;
  readOnly?: boolean;
}

/**
 * Represents a set of data to be used for editing or creating configuration options
 */
export interface RbdConfigurationSection {
  heading: string;
  class: string;
  options: RbdConfigurationExtraField[];
}
