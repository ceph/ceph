export const StepTitles = {
  ChooseMirrorPeerRole: $localize`Choose mirror peer role`,
  SelectFilesystem: $localize`Select filesystem`,
  CreateOrSelectEntity: $localize`Create or select entity`,
  GenerateBootstrapToken: $localize`Generate token`,
  ImportBootstrapToken: $localize`Import token`,
  Review: $localize`Review`
} as const;

export const STEP_TITLES_MIRRORING_REMOTE = [
  StepTitles.ChooseMirrorPeerRole,
  StepTitles.SelectFilesystem,
  StepTitles.CreateOrSelectEntity,
  StepTitles.GenerateBootstrapToken,
  StepTitles.Review
];

export const STEP_TITLES_MIRRORING_LOCAL = [
  StepTitles.ChooseMirrorPeerRole,
  StepTitles.SelectFilesystem,
  StepTitles.ImportBootstrapToken,
  StepTitles.Review
];

export const LOCAL_ROLE = 'local';
export const REMOTE_ROLE = 'remote';
