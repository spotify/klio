#!/bin/sh

# Decrypt the file
mkdir -p $HOME/.config/gcloud
# --batch to prevent interactive command
# --yes to assume "yes" for questions
gpg --quiet --batch --yes --decrypt --passphrase="$IT_GCP_CREDS_KEY" \
--output $HOME/.config/gcloud/application_default_credentials.json klio-gh-actions-creds.json.gpg