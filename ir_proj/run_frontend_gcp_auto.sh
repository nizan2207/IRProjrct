
INSTANCE_NAME="instance-1"
REGION=us-central1
ZONE=us-central1-c
PROJECT_NAME="irproject-206655839"
IP_NAME="$PROJECT_NAME-ip"
GOOGLE_ACCOUNT_NAME="zehaviam" # without the @post.bgu.ac.il or @gmail.com part

# 0. Install Cloud SDK on your local machine or using Could Shell
# check that you have a proper active account listed
gcloud auth list
gcloud config set project $PROJECT_NAME
gcloud config set compute/zone $ZONE

# 1. Set up public IP
gcloud compute addresses create $IP_NAME --project=$PROJECT_NAME --region=$REGION
#compute the address and put it in the variable
INSTANCE_IP=$(gcloud compute addresses list | grep ADD | awk '{print $2}')

# 2. Create Firewall rule to allow traffic to port 8080 on the instance
gcloud compute firewall-rules create default-allow-http-8080 --allow tcp:8080 --source-ranges 0.0.0.0/0 --target-tags http-server

# 3. Create the instance. Change to a larger instance (larger than e2-micro) as needed.
gcloud compute instances create $INSTANCE_NAME --zone=$ZONE --machine-type=e2-micro --network-interface=address=$INSTANCE_IP,network-tier=PREMIUM,subnet=default --metadata-from-file startup-script=startup_script_gcp.sh --scopes=https://www.googleapis.com/auth/cloud-platform --tags=http-server

sleep 5m

# 4. Secure copy your app to the VM
gcloud compute scp search_frontend.py $GOOGLE_ACCOUNT_NAME@$INSTANCE_NAME:/home/$GOOGLE_ACCOUNT_NAME                                                                                                      100%   44KB 143.6KB/s   00:00    
gcloud compute scp IdTitle.pickle  $GOOGLE_ACCOUNT_NAME@$INSTANCE_NAME:/home/$GOOGLE_ACCOUNT_NAME
gcloud compute scp index.pickle   $GOOGLE_ACCOUNT_NAME@$INSTANCE_NAME:/home/$GOOGLE_ACCOUNT_NAME

# 5. SSH to your VM and start the app
gcloud compute ssh $GOOGLE_ACCOUNT_NAME@$INSTANCE_NAME

# run the app
python3 search_frontend.py

