pipeline {
    agent { dockerfile true }
    stages {
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
