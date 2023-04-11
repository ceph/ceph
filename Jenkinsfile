pipeline {
  agent {
    dockerfile {
      label 'docker'
      filename 'Dockerfile.build'
      additionalBuildArgs '--build-arg JENKINS_HOME=$JENKINS_HOME'
      args '-v $HOME/.ccache:/home/jenkins/.ccache'
    }
  }

  options {
    timestamps()
    disableConcurrentBuilds(abortPrevious: true)
    timeout(time: 2, unit: 'HOURS')
  }


  stages {
    stage('Configure') {
      steps {
        runMakeCheck 'configure'
      }
    }

    stage('Build') {
      steps {
        sh '. ./run-make-check.sh && build vstart'
      }
    }

    stage('Test') {
      parallel {
        stage('Unit Tests') {
          steps {
            sh '. ./run-make-check.sh && build tests'
            sh '. ./run-make-check.sh && cd build && run'
          }
          post {
            always{
              xunit(
                thresholds: [skipped(failureThreshold: '0'), failed(failureThreshold: '0')],
                tools: [CTest(
                  deleteOutputFiles: true,
                  failIfNotNew: true,
                  pattern: 'build/Testing/**/Test.xml',
                  skipNoTestFiles: true,
                  stopProcessingIfError: true)
                ]
              )
            }
          }
        }
      } // parallel
    } // stage('Test')

  } // stages
} // pipeline

def runMakeCheck(String command) {
  sh """
  #!/usr/bin/env bash
  set -exo pipefail
  source ./run-make-check.sh
  ${command}
  """
}
