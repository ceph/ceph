export interface SmartAttribute {
  flags: {
    auto_keep: boolean;
    error_rate: boolean;
    event_count: boolean;
    performance: boolean;
    prefailure: boolean;
    string: string;
    updated_online: boolean;
    value: number;
  };
  id: number;
  name: string;
  raw: { string: string; value: number };
  thresh: number;
  value: number;
  when_failed: string;
  worst: number;
}

/**
 * The error structure returned from the back-end if SMART data couldn't be
 * retrieved.
 */
export interface SmartError {
  dev: string;
  error: string;
  nvme_smart_health_information_add_log_error: string;
  nvme_smart_health_information_add_log_error_code: number;
  nvme_vendor: string;
  smartctl_error_code: number;
  smartctl_output: string;
}

/**
 * Common smartctl output structure.
 */
interface SmartCtlOutput {
  argv: string[];
  build_info: string;
  exit_status: number;
  output: string[];
  platform_info: string;
  svn_revision: string;
  version: number[];
}

/**
 * Common smartctl device structure.
 */
interface SmartCtlDevice {
  info_name: string;
  name: string;
  protocol: string;
  type: string;
}

/**
 * smartctl data structure shared among HDD/NVMe.
 */
interface SmartCtlBaseDataV1 {
  device: SmartCtlDevice;
  firmware_version: string;
  json_format_version: number[];
  local_time: { asctime: string; time_t: number };
  logical_block_size: number;
  model_name: string;
  nvme_smart_health_information_add_log_error: string;
  nvme_smart_health_information_add_log_error_code: number;
  nvme_vendor: string;
  power_cycle_count: number;
  power_on_time: { hours: number };
  serial_number: string;
  smart_status: { passed: boolean; nvme?: { value: number } };
  smartctl: SmartCtlOutput;
  temperature: { current: number };
  user_capacity: { blocks: number; bytes: number };
}

export interface RVWAttributes {
  correction_algorithm_invocations: number;
  errors_corrected_by_eccdelayed: number;
  errors_corrected_by_eccfast: number;
  errors_corrected_by_rereads_rewrites: number;
  gigabytes_processed: number;
  total_errors_corrected: number;
  total_uncorrected_errors: number;
}

/**
 * Result structure of `smartctl` applied on an SCSI. Returned by the back-end.
 */
export interface IscsiSmartDataV1 extends SmartCtlBaseDataV1 {
  scsi_error_counter_log: {
    read: RVWAttributes[];
  };
  scsi_grown_defect_list: number;
}

/**
 * Result structure of `smartctl` applied on an HDD. Returned by the back-end.
 */
export interface AtaSmartDataV1 extends SmartCtlBaseDataV1 {
  ata_sct_capabilities: {
    data_table_supported: boolean;
    error_recovery_control_supported: boolean;
    feature_control_supported: boolean;
    value: number;
  };
  ata_smart_attributes: {
    revision: number;
    table: SmartAttribute[];
  };
  ata_smart_data: {
    capabilities: {
      attribute_autosave_enabled: boolean;
      conveyance_self_test_supported: boolean;
      error_logging_supported: boolean;
      exec_offline_immediate_supported: boolean;
      gp_logging_supported: boolean;
      offline_is_aborted_upon_new_cmd: boolean;
      offline_surface_scan_supported: boolean;
      selective_self_test_supported: boolean;
      self_tests_supported: boolean;
      values: number[];
    };
    offline_data_collection: {
      completion_seconds: number;
      status: { string: string; value: number };
    };
    self_test: {
      polling_minutes: { conveyance: number; extended: number; short: number };
      status: { passed: boolean; string: string; value: number };
    };
  };
  ata_smart_error_log: { summary: { count: number; revision: number } };
  ata_smart_selective_self_test_log: {
    flags: { remainder_scan_enabled: boolean; value: number };
    power_up_scan_resume_minutes: number;
    revision: number;
    table: {
      lba_max: number;
      lba_min: number;
      status: { string: string; value: number };
    }[];
  };
  ata_smart_self_test_log: { standard: { count: number; revision: number } };
  ata_version: { major_value: number; minor_value: number; string: string };
  in_smartctl_database: boolean;
  interface_speed: {
    current: {
      bits_per_unit: number;
      sata_value: number;
      string: string;
      units_per_second: number;
    };
    max: {
      bits_per_unit: number;
      sata_value: number;
      string: string;
      units_per_second: number;
    };
  };
  model_family: string;
  physical_block_size: number;
  rotation_rate: number;
  sata_version: { string: string; value: number };
  smart_status: { passed: boolean };
  smartctl: SmartCtlOutput;
  wwn: { id: number; naa: number; oui: number };
}

/**
 * Result structure of `smartctl` returned by Ceph and then back-end applied on
 * an NVMe.
 */
export interface NvmeSmartDataV1 extends SmartCtlBaseDataV1 {
  nvme_controller_id: number;
  nvme_ieee_oui_identifier: number;
  nvme_namespaces: {
    capacity: { blocks: number; bytes: number };
    eui64: { ext_id: number; oui: number };
    formatted_lba_size: number;
    id: number;
    size: { blocks: number; bytes: number };
    utilization: { blocks: number; bytes: number };
  }[];
  nvme_number_of_namespaces: number;
  nvme_pci_vendor: { id: number; subsystem_id: number };
  nvme_smart_health_information_log: {
    available_spare: number;
    available_spare_threshold: number;
    controller_busy_time: number;
    critical_comp_time: number;
    critical_warning: number;
    data_units_read: number;
    data_units_written: number;
    host_reads: number;
    host_writes: number;
    media_errors: number;
    num_err_log_entries: number;
    percentage_used: number;
    power_cycles: number;
    power_on_hours: number;
    temperature: number;
    temperature_sensors: number[];
    unsafe_shutdowns: number;
    warning_temp_time: number;
  };
  nvme_total_capacity: number;
  nvme_unallocated_capacity: number;
}

/**
 * The shared fields each result has after it has been processed by the front-end.
 */
interface SmartBasicResult {
  device: string;
  identifier: string;
}

/**
 * The SMART data response structure of the back-end. Per device it will either
 * contain the structure for a HDD, NVMe or an error.
 */
export interface SmartDataResponseV1 {
  [deviceId: string]: AtaSmartDataV1 | NvmeSmartDataV1 | SmartError;
}

/**
 * The SMART data result after it has been processed by the front-end.
 */
export interface SmartDataResult extends SmartBasicResult {
  info: { [key: string]: any };
  smart: {
    attributes?: any;
    data?: any;
    nvmeData?: any;
    scsi_error_counter_log?: any;
    scsi_grown_defect_list?: any;
  };
}

/**
 * The SMART error result after is has been processed by the front-end. If SMART
 * data couldn't be retrieved, this is the structure which is returned.
 */
export interface SmartErrorResult extends SmartBasicResult {
  error: string;
  smartctl_error_code: number;
  smartctl_output: string;
  userMessage: string;
}
