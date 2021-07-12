# Integration testing using the behave framework


## Introduction

Behave framework is based on the Behaviour driven development where the test cases defined using gherkin langauge (written natural language style). The test cases are defined in .feature files in `feature` directory and python implementation defined under `/feature/steps`.

`features/environment.py` file is used to set up environment for testing the scenario using the kcli tool. When behave command is execute before each feature, kcli plan is generated to create the virtual machines.

## Executing the behave tests

```bash
$ tox
```