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

## Note on steps that check for results in output

Steps that checks for results in output do not match full strings. The output provided to check for is split up
by line then each line is split up into a set of tokens by splitting the line on whitespace. The same process
is done for the actual command output. Then, for each line of tokens to check for, we look at each line of tokens
from the command output to find a line that contains ALL the tokens in the line we are checking for. This system
is flexible and avoids having to worry as much about the format of output and allows you to check for partial lines
very easily. It does not work in cases where a success/failure case is differentiated by the order certain words in
the output show up; only in cases where the output is actually different and there are certain keywords that can be
checked for correctness. An additional syntax for checking for exact lines and for specifically checking certain
keywords are NOT present in the output is future work that should be done here.

## Specifying cluster attributes at the start of a feature

These tests allow you to specify a number of configuration options for the cluster the test will be run on at the
start of the test. Currently, these options and their syntax are

  -------------------------------------------------------------------------
  NODES, used to specify number of nodes in ceph cluster

  NODES | <value>

  Ex:
      NODES | 2
  -------------------------------------------------------------------------
  
  -------------------------------------------------------------------------
  BOOTSTRAP_FLAG, for setting miscellaneous bootstrap flags for cluster

  BOOTSTRAP_FLAG | <flag-name> | <value>

  Ex:
      BOOTSTRAP_FLAG | skip-monitoring-stack | True
  -------------------------------------------------------------------------

  -------------------------------------------------------------------------
  CEPH_CONFIG, for setting initial ceph config values to be assimilated
  during bootstrap

  CEPH_CONFIG | <section> | <param> | <value>

  Ex:
      CEPH_CONFIG | mgr | mgr/cephadm/use_agent | false

      would translate to

      [mgr]
      mgr/cephadm/use_agent = false
  -------------------------------------------------------------------------
  
In the future, hopefully there will be an option to specify needing a fresh set of VMs or certain settings for the VMS
(for tests that test special configuration settings like only using ipv6) so that that those things can be tested in fresh
VMs while other tests can just re-use VMs for more efficient testing time.