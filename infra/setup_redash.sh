gcloud compute images create "redash-8-0-0" --source-uri gs://redash-images/redash.8.0.0-b32245-1.tar.gz
gcloud compute instances create redash --image redash-8-0-0

