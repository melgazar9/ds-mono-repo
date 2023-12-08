docker build -t gcr.io/t-emissary-368916/financial-elt:v1 .

#######################
### May need to run ###
#######################

# sudo chmod 660 /var/run/docker.sock
# docker push gcr.io/t-emissary-368916/financial-elt:v1
# gcloud run deploy financial-elt-dev --image gcr.io/t-emissary-368916/financial-elt:v1 --platform managed --region us-central1

#######################

docker push gcr.io/t-emissary-368916/financial-elt:v1

gcloud run deploy --image gcr.io/t-emissary-368916/financial-elt:v1 --platform managed

gcloud projects add-iam-policy-binding t-emissary-368916 --member=serviceAccount:service-1@gcp-sa-cloudscheduler.iam.gserviceaccount.com --role=roles/run.invoker
