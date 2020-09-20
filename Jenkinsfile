#!/usr/bin/env groovy

pipeline {
    agent {
      label 'huge && bionic && x86_64 && !xenial && !trusty'
    }
    environment {
        NPROC = "0"
    }

    stages {
        stage('Prepare') {
            steps {
                sh "sudo ./install-deps.sh"
            }
        }
        
        stage('Build') {
            steps {
                sh "./run-make-check.sh"
            }
        }
    }

    post { 
        always { 
            cleanWs()
        }
    }
}
