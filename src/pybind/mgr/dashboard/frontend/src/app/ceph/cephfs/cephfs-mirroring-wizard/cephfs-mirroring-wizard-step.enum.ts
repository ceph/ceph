export const StepTitles = {
  ChooseMirrorPeerRole: $localize`Choose mirror peer role`,
  SelectFilesystem: $localize`Select filesystem`,
  CreateOrSelectEntity: $localize`Create or select entity`,
  GenerateBootstrapToken: $localize`Generate bootstrap token`,
  Review: $localize`Review`
} as const;

export const STEP_TITLES_MIRRORING_CONFIGURED = [
  StepTitles.ChooseMirrorPeerRole,
  StepTitles.SelectFilesystem,
  StepTitles.CreateOrSelectEntity,
  StepTitles.GenerateBootstrapToken
];

export const LOCAL_ROLE = 'local';
export const REMOTE_ROLE = 'remote';
