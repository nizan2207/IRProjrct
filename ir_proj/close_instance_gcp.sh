INSTANCE_NAME="instance-1"
REGION=us-central1
ZONE=us-central1-c
PROJECT_NAME="irproject-206655839"
IP_NAME="$PROJECT_NAME-ip"
GOOGLE_ACCOUNT_NAME="zehaviam" # without the @post.bgu.ac.il or @gmail.com part


################################################################################
# Clean up commands to undo the above set up and avoid unnecessary charges
gcloud compute instances delete -q $INSTANCE_NAME
# make sure there are no lingering instances
gcloud compute instances list
# delete firewall rule
gcloud compute firewall-rules delete -q default-allow-http-8080
# delete external addresses
gcloud compute addresses delete -q $IP_NAME --region $REGION