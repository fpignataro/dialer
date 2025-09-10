The OmniDialer for Omnileads (OML), a dialer designed to be integrated to OML and to be a FLOSS option to Wombat.

Usage:

```
$ cp env .env
$ docker-compose build
$ docker-compose up -d
```

A flask server would be running at 0.0.0.0:1440 with a Gearman job server and the required Gearman workers.

Do:

$ docker run --rm -itd -p 4731:4731 --network=<oml-docker-network> --name=gearman_job_server_1 artefactual/gearmand:1.1.18-alpine

changing the ports and container name to add Gearman job servers, you will need to add it to the settings as well by modifying the environment variable GEARMAN_JOB_SERVERS

You can also add more workers in the same host by doing:

docker run --rm -itd --network=<oml-docker-network> --name=omnidialer-worker-n omnidialer_worker

... and in a different host by modifying the .env setting GEARMAN_JOB_SERVERS pointing to the IP address where Omnidialer is running and you can add more Gearman job servers, if available, by adding more pair <host>:<ip> and using the separator | . After that you can spawn the worker by executing:

docker-compose -f docker-compose-single-worker.yml up -d

It is also possible to customize the jobs that will be accepted inside the worker instances by modifying the .env setting GEARMAN_JOBS

See the files at 'testing/restclient' & 'testing/curls'

The workflow would be for now:
- Hit create campaign endpoint
- Hit start campaign
and
- Hit pause campaign and resume campaign endpoints according to your needs.

Troubleshooting:

- If artefactual/gearmand:1.1.18-alpine does not run in Mac M1, but you can build the image from their repository manually and use it directly (https://github.com/artefactual-labs/docker-gearmand)

- If you are on another machine you need to modify the settings ASTERISK_HOST, REDIS_OML_SERVER and POSTGRES_OML_SERVER to point to the IP of OML's host.  Also, you need to modify the 'ari.conf' in the container 'oml-asterisk_dialer' to add your machine IP to the 'allowed_origins' in the [general] configuration section. After that, run 'reload' in the Asterisk console.

- If some for some reason (maybe some unexpected error) you are starting a campaign and the process-campaign shows a message like: "Campaign <id_campaign>: is already running" you need to clear the Redis lock associated with that campaign, if there are many unexpected locks you can run the script  utility 'remove-locks.py' inside any of the containers for the job 'process-campaign' for delete those Redis keys.

Unit tests:

For run the unit tests just do:

bash run-tests.bash

Note: pass an argument if you don't want to rebuild the code for changes

bash run-tests.bash no-build
