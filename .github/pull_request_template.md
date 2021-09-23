

<!--
  - Please give your pull request a title like

      [component]: [short description]

  - Please use this format for each git commit message:

      [component]: [short description]

      [A longer multiline description]

      Fixes: [ticket URL on tracker.ceph.com, create one if necessary]
      Signed-off-by: [Your Name] <[your email]>

    For examples, use "git log".

  - The Signed-off-by line in every git commit is important; see SubmittingPatches.rst.
-->

## Checklist
- Tracker (select at least one)
  - [ ] Very recent bug; references commit where it was introduced
  - [ ] New feature (no ticket)
  - [ ] References tracker ticket
  - [ ] Doc update (no ticket)
- Documentation (select at least one)
  - [ ] Updates relevant documentation
  - [ ] No doc update is appropriate
- Tests (select at least one)
  - [ ] Includes unit test(s)
  - [ ] Includes integration test(s)
  - [ ] Includes bug reproducer
  - [ ] No tests
- Teuthology
  - [ ] Completed teuthology run
  - [ ] No teuthology test necessary (e.g., documentation)
