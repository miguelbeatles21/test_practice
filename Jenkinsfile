pipeline {
    stages {
        stage ('Build Image'){
            agent {
                dockerfile {
                    filename 'Dockerfile',
                    path_initial '/test'
                } 
            }
        }
        stage('Test') {
            steps {
                echo 'Testing'
                sh 'pytest'
            }
        }
        stage('Scanning') {
            steps {
                echo 'Init scanning sonarq'
                
            }
        }

    }
}
