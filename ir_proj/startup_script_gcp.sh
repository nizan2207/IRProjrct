# Echo commands
set -v
sudo apt-get update
sudo apt-get update -qq
sudo apt install openjdk-8-jdk-headless -qq
spark_jars='/usr/local/lib/python3.7/dist-packages/pyspark/jars'
graphframes_jar='https://repos.spark-packages.org/graphframes/graphframes/0.8.2-spark3.2-s_2.12/graphframes-0.8.2-spark3.2-s_2.12.jar'
sudo wget -N -P $spark_jars $graphframes_jar
sudo apt-get install -yq git python3 python3-setuptools python3-dev build-essential
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
sudo python3 get-pip.py
sudo pip3 install --no-input nltk==3.6.3 Flask==2.0.2 --no-cache-dir flask-restful==0.3.9 numpy==1.21.4 google-cloud-storage==1.43.0 pandas==1.3.5 spark==3.1.2

