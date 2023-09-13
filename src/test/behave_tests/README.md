# Integration testing using the behave framework


## Introduction

Behave framework is based on the Behaviour driven development where the test cases defined using gherkin language (written natural language style). The test cases are defined in .feature files in `feature` directory and python implementation defined under `/feature/steps`.

`features/environment.py` file is used to set up environment for testing the scenario using the kcli tool. When behave command is execute before each feature, kcli plan is generated to create the virtual machines.

## Issues

* We can't run the behave test cases via tox command.

## Executing the behave tests

We can execute all test scenario's by executing `behave` command under `src/test/behave_test` where `features` directory is required.

```bash
$ behave
```

## Executing the behave tests with tags

Tag's can be used to execute only specific type of test scenario's.

```bash
$ behave -t <tag_name>
```

We have included the following tag for implemented test cases.
* osd
* ceph_shell
* cephadm

## Steps used to define the test scenarios

Python implementation of steps are defined in `steps` directory under `src/test/behave_tests/features/`.
Following implemented gherkin language steps used in `.feature` files to define the test scenarios.

@given steps
* __I log as root into {`vm_name`}__ (vm_name is name of virtual machine)
* __I execute in {`shell`}__ (shell should defined as `host` or `cephadm_shell`)

@when steps
* __I execute in {`shell`}__

@then steps
* __I execute in {`shell`}__
* __I wait for {`timeout`} seconds until I get__ (timeout should be defined in seconds)
* __I get results which contain__
