export interface UpgradeInfoInterface {
  image: string;
  registry: string;
  versions: string[];
}

export interface UpgradeStatusInterface {
  target_image: string;
  in_progress: boolean;
  which: string;
  services_complete: string;
  progress: string;
  message: string;
  is_paused: boolean;
}
