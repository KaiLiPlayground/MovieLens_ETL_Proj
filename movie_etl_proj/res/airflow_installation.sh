sudo su

mkdir -p ~/p2/airflow

python3 -m venv ~/p2/airflow/venv
source ~/p2/airflow/venv/bin/activate


AIRFLOW_VERSION=2.8.1
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[crypto,postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Additional dependencies if needed
sudo pip install six --upgrade
sudo pip install markupsafe --upgrade

# Set up environment variables
echo 'export PATH=/usr/local/bin:$PATH' >> /root/.bash_profile
source /root/.bash_profile

# Initialize Airflow
airflow initdb

# Configure Airflow settings
sed -i '/sql_alchemy_conn/s/^/#/g' ~/airflow/airflow.cfg
sed -i '/sql_alchemy_conn/c\sql_alchemy_conn = postgresql://airflow:airflowpassword@airflowstack2024-dbinstance-vc9uopllco1y.cgppbdvzhe7r.ap-southeast-2.rds.amazonaws.com:5432/airflowdb
sed -i '/executor = SequentialExecutor/s/^/#/g' ~/airflow/airflow.cfg
sed -i '/executor = SequentialExecutor/ a executor = LocalExecutor' ~/airflow/airflow.cfg
airflow initdb

# install awscli 
pip install --upgrade --user awscli


# Download and unzip the Movielens dataset
wget http://files.grouplens.org/datasets/movielens/ml-latest.zip && unzip ml-latest.zip
        
aws s3 cp ml-latest s3://kai-airflow-storage --recursive

sudo pip install boto3


# for all installation of py libraries, make sure to clear the conflicts by checking "pip check"

apt install -y git
# Clone the git repository
git clone https://github.com/aws-samples/aws-concurrent-data-orchestration-pipeline-emr-livy.git

 # Move all the files to the ~/airflow directory. The Airflow config file is setup to hold all the DAG related files in the ~/airflow/ folder.
mv aws-concurrent-data-orchestration-pipeline-emr-livy/* ~/airflow/
# Delete the higher-level git repository directory
rm -rf aws-concurrent-data-orchestration-pipeline-emr-livy
# Replace the name of the S3 bucket in each of the .scala files. CHANGE THE HIGHLIGHTED PORTION BELOW TO THE NAME OF THE S3 BUCKET YOU CREATED IN STEP 1. The below command replaces the instance of the string ‘<s3-bucket>’ in each of the scripts to the name of the actual bucket.
sed -i 's/<s3-bucket>/kai-airflow-storage/g' /root/airflow/dags/transform/*

# run airflow scheduler to start the DAGs
airflow scheduler

# Run Airflow webserver
airflow webserver

# set user and password once airflow is up and running 
# airflow users create \
#    --username admin \
#    --firstname FIRST_NAME \
#    --lastname LAST_NAME \
#    --role Admin \
#    --email admin@example.com
airflow users create \
    --username kaiadmin \
    --firstname Kai \
    --lastname Li \
    --role Admin \
    --email your_email@example.com


# restart airflow services 

# kill all airflow processes 
ps aux | grep 'scheduler' | awk '{print $2}' | xargs -r sudo kill -9

# kill all worker 
lsof -i :8793 | awk 'NR>1 {print $2}' | xargs sudo kill -9
