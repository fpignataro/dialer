#!/usr/bin/env bash

echo starting dedicated containers ...

docker-compose up -d --build

for i in {1..3}; do
    bash add-worker-single-job.bash process-contact process-contact-$i $1
done

bash add-worker-single-job.bash process-campaign process-campaign-1 $1

bash add-worker-single-job.bash create-campaign create-campaign-1 $1

bash add-worker-single-job.bash resume-campaign resume-campaign-1 $1

bash add-worker-single-job.bash edit-campaign edit-campaign-1 $1

bash add-worker-single-job.bash stop-campaign stop-campaign-1 $1

bash add-worker-single-job.bash pause-campaign pause-campaign-1 $1

bash add-worker-single-job.bash delete-campaign delete-campaign-1 $1

bash add-worker-single-job.bash process-event process-event-1 $1

bash add-worker-single-job.bash schedule-contact schedule-contact-1 $1

bash add-worker-single-job.bash send-reports send-reports-1 $1

bash add-worker-single-job.bash add-incidence-rule-disposition add-incidence-rule-disposition-1 $1

bash add-worker-single-job.bash create-incidence-rule create-incidence-rule-1 $1

bash add-worker-single-job.bash delete-incidence-rule delete-incidence-rule-1 $1

bash add-worker-single-job.bash update-incidence-rule update-incidence-rule-1 $1

bash add-worker-single-job.bash schedule-agenda schedule-agenda-1 $1

bash add-worker-single-job.bash change-database change-database-1 $1

bash add-worker-single-job.bash render-template render-template-1 $1

bash add-worker-single-job.bash stop-dialer stop-dialer-1 $1
