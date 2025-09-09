General description
===================

Omnidialer(OMD) is a dialer software meant to be a FLOSS component for Omnileads (OML) that can be used as a drop in alternative to Wombat dialer.

It is implemented in Python and use components such as Docker, docker-compose, Postgres, Redis and Gearman.

The use of Gearman allows to easily horizontal scale the system by adding Gearman workers dedicated to the most time & memory consuming resources.

The system is designed to run on every GNU/Linux system that supports bash, docker & docker-compose.

It can run in the same host OML is running or on an external host by configuring the environment variables present in the _env_ file.
The relevant environment variables are in this case: *REDIS_OML_SERVER*, *REDIS_OML_PORT*, *ASTERISK_APP*, *ASTERISK_USER*, *ASTERISK_PASS*, *ASTERISK_HOST*, *ASTERISK_PORT*, *DIALER_ACD_HOST*, *POSTGRES_OML_PASSWORD*, *POSTGRES_OML_SERVER* and *POSTGRES_OML_PORT*.

Usage:

Make sure you have Docker & Docker-Compose installed and that you have bash shell available.

Then do:

```
cp env .env
docker-compose build
docker-compose up -d
```

A flask server would be running at 0.0.0.0:1440 with a Gearman job server and the required Gearman workers.

After this you can hit the differents endpoints to interact with the system. Take a look at the folder 'test/curls' for a few examples.

Endpoints explanation
=====================

Create campaign
---------------

### Endpoint: `[POST] /create-campaign/<id_campaign>`

#### Description
The create-campaign endpoint creates a campaign inside OMD by importing the related data from a campaing of OML.

The value <id_campaign> correspond to the id of a campaign from OML, it will be also the id of the new campaign in OMD.

The other values imported from OML are the fields _estado_,_nombre_,_fecha_inicio_,_fecha_fin_,_control_de_duplicados_ and _prioridad_ from the table _ominicontacto_app_campana_ that will be copied to the table _campaign_. Aditionally the entries realtives to incidence rules and contacts are also imported to the tables _incidence_rules_, _incidence_rules_disposition_, contact_in_campaign & contact respectively.

The contact_strategy JSON parameter is also added as a field of the table _campaign_. This will be used in future to customize the way the dialer calls more than one time to a contact in the current campaign.

#### Method
- **HTTP Method:** `POST`

### Request Body
```json
{
  "contact-strategy": "[<id_strategy1>, <id_strategy2>, ... ,<id_strategy_n>]"
}
```


Edit campaign
---------------

### Endpoint: `[POST] /edit-campaign/<id_campaign>`

#### Description
The create-campaign endpoint edits a campaign inside OMD by modifying the related data with the current values of the campaign with the same id in OML.

The value <id_campaign> correspond to the id of a campaign in OML and OMD.

The tables _campaign_, _incidence_rules_ and _incidence_rules_disposition_ could be modified according to the existent data in OML.

#### Method
- **HTTP Method:** `POST`

### Request Body
```json
{
  "contact-strategy": "[<id_strategy1>, <id_strategy2>, ... ,<id_strategy_n>]"
}
```


Start campaign
---------------

### Endpoint: `[POST] /start-campaign/<id_campaign>`

#### Description
The start-campaign endpoint initiates the main process of the campaign, it will set the field 'dialer_status' to status ACTIVE and will start a loop that will call contacts from the OMD database according to the configuration of the campaign and the agents available in OML.

The value <id_campaign> correspond to the id of a campaign in OML and OMD.

#### Method
- **HTTP Method:** `POST`



Pause campaign
---------------

### Endpoint: `[POST] /pause-campaign/<id_campaign>`

#### Description
The pause-campaign endpoint pause the process of campaign in OMD by modifying the field _dialer_status_ of the campaign entry to the value _PAUSED_.

The value <id_campaign> correspond to the id of a campaign in OML and OMD.

#### Method
- **HTTP Method:** `POST`


Resume campaign
---------------

### Endpoint: `[POST] /resume-campaign/<id_campaign>`

#### Description
The resume-campaign endpoint resumes the process of campaign in OMD by modifying the field _dialer_status_ of the campaign entry to the value _RESUMED_ and will restart the campaign process.

The value <id_campaign> correspond to the id of a campaign in OML and OMD.

#### Method
- **HTTP Method:** `POST`

Stop campaign
---------------
### Endpoint: `[POST] /post-campaign/<id_campaign>`

#### Description
The post-campaign endpoint stops a running campaign by first set the _dialer_status_ field to status FINALIZED and after that it will copy all the entries related to the campaign to the historic tables. Entries are moved to tables _campaign_historic_, _contact_in_campaign_historic_, _incidence_rules_historic_ and _incidence_rules_disposition_historic_ . Finally the original campaign and the related tables are removed from the DB as if it were using the endpoint delete-campaign.


Delete campaign
---------------

### Endpoint: `[POST] /delete-campaign/<id_campaign>`

#### Description
The delete-campaign endpoint first pauses a campaign and then remove it completely from the DB; the tables _campaign_, _incidence_rules_, _incidence_rules_disposition_ and _contact_in_campaign_ can be affected with this endpoint.

The value <id_campaign> correspond to the id of a campaign in OML and OMD.

#### Method
- **HTTP Method:** `POST`


Add disposition for contact
---------------------------

### Endpoint: `[POST] /add-incidence-rule-disposition/<id_campaign>`

#### Description
The add-incidence-rule-disposition endpoint is meant to be used by OML to signal that a disposition option was added to a contact in the campaign and that an incidence rule should be analyzed in this case. OMD will add the information about the disposition to the contact history in the campaign and if the linked incidence rule matches it will schedule a call for the contact.

The value <id_campaign> correspond to the id of a campaign in OML and OMD.

#### Method
- **HTTP Method:** `POST`

### Request Body
```json
{
        "id_contact": <id_contact>,
        "disposition_option": <id_disposition_option>
}
```

Create incidence rule
---------------------

### Endpoint: `[POST] /create-incidence-rule/<id_campaign>`

#### Description
The create-incidence-rule endpoint will add a new incidence rule to a campaign

The value <id_campaign> correspond to the id of a campaign in OML and OMD.

Other parameters are:
<id_rule> - id of the incidence_rule in OML
<max_attempt> - maximum number of attempts
<retry_later> - delay (in seconds) before any attempt
<mode> - mode: fixed or multinum
<type> - 1 (to create an status-like incidence rule) or 2 (to create an disposition-like incidence rule)
<status> - id of the status (only if <type> is 1)
<status_custom> - name of the status (only if <type> is 1)
<disposition_option_id> - id of the disposition option applied in OML (only if <type> is 2)


#### Method
- **HTTP Method:** `POST`

### Request Body
```json
{
        "id_rule": 3,
        "status": 3,
        "status_custom": "no answer",
        "max_attempt": 5,
        "retry_later": 5,
        "mode": 1,
        "type": 1

}
```

Update incidence rule
---------------------

### Endpoint: `[POST] /update-incidence-rule/<id_campaign>`

#### Description
The update-incidence-rule endpoint will modify an existing incidence rule in a campaign

The value <id_campaign> correspond to the id of a campaign in OML and OMD.

Other parameters are:
<id_rule> - id of the incidence_rule in OML
<max_attempt> - maximum number of attempts
<retry_later> - delay (in seconds) before any attempt
<mode> - mode: fixed or multinum
<type> - 1 (to update an status-like incidence rule) or 2 (to update an disposition-like incidence rule)
<status> - id of the status (only if <type> is 1)
<status_custom> - name of the status (only if <type> is 1)
<disposition_option_id> - id of the disposition option applied in OML (only if <type> is 2)


#### Method
- **HTTP Method:** `POST`

### Request Body
```json
{
        "id_rule": 3,
        "status": 3,
        "status_custom": "no answer",
        "max_attempt": 5,
        "retry_later": 5,
        "mode": 1,
        "type": 1

}
```

Delete incidence rule
---------------------

### Endpoint: `[POST] /delete-incidence-rule/<id_campaign>`

#### Description
The delete-incidence-rule endpoint will delete an incidence rule in a campaign

The value <id_campaign> correspond to the id of a campaign in OML and OMD.

Other parameters are:
<id> - id of the incidence_rule in OML
<type> - mode: 1 for status-like incidence rule, 2 for disposition-like incidence rule


#### Method
- **HTTP Method:** `POST`

### Request Body
```json
{
        "id": 3,
        "type": 1
}
```


Add agenda to call
------------------

### Endpoint: `[POST] /add-agenda/<id_campaign>`

#### Description
The add-agenda endpoint will schedule a call to be made at a certain date and time

The value <id_campaign> correspond to the id of a campaign in OML and OMD.

Other parameters are:
<id_contact> - the contact to be called
<campaign_name> - the name of the campaign in OML
<phone_number> - phone number to be called
<datetime> - the date and time the call will be placed


#### Method
- **HTTP Method:** `POST`

### Request Body
```json
{
        "id_contact": 7,
        "campaign_name": "clonan_33",
        "phone_number": "313123427",
        "datetime": "27/11/24 15:42:00"
}
```

Change contacts database
------------------

### Endpoint: `[POST] /change-database/<id_campaign>`

#### Description
The change-database endpoint will pause the campaign and replace the existing contacts in the campaign with the newly changed in its counterparts campaign in OML.
It will also remove all the reports and history associated with the campaign.

The value <id_campaign> correspond to the id of a campaign in OML and OMD.

#### Method
- **HTTP Method:** `POST`


Arquitecture
============

The arquitecture of the system is shown in the following diagram:

![alt text](images/omnidialer-arquitecture.svg "Omnidialer arquitecture")

The system serves the endpoints with a Flask server that, in turn redirects the tasks to the running Gearman workers.

There is also a websocket server that will receive the ARI events linked to the calls and will redirect the task to a Gearman worker.

The data of the system is persisted in a Postgres instance and some data are replicated on a Redis instance.

The data saved in Redis is related with contact history in a campaign and reports of the campaign.

The system also publish to a PUBSUB channel information about reports, events and status of the campaigns, users can subscribe to OML:CHANNEL:DIALER to get this information.

It is also possible to places agendas for calls using a custom scheduler.

There should be a worker for process-campaign for every campaign dialing in parallel.

The system also includes a simple admin that allows some degree of management on the campaigns and allow to manipulate the general status of the system (start, stop and restart) in gracefully way.

Horizontal scalability
======================

The system is designed with the ability to horizontal scale by simply creating more Gearman job servers and workers.

To add more gearman job servers you will need to modify the environment variable GEARMAN_JOB_SERVERS and add a new job server in the following way:

$ docker run --rm -itd -p 4731:4731 --network=<oml-docker-network> --name=gearman_job_server_2 artefactual/gearmand:1.1.18-alpine

You can also add more workers in the same host by using the following way:

$ bash add-worker-single-job.bash <name_of_the_gearman_job_to_serve> <new_container_name> <oml-docker-network>

for example:

$ bash add-worker-single-job.bash process-contact process-contact-5 omnileads_omnileads

If you are in other host you can run the docker-compose file _docker-compose-single-worker.yml_ after setting the relevant values in the .env file and after that you can more workers as needed

Tests
=====

For run the unit tests just do:

$ bash run-tests.bash

Logging
=======

If you want to see debug logs on every single worker you need to change the environment variable PYTHON_LOGLEVEL from _warning_ to _debug_
