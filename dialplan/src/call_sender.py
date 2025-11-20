#!/usr/bin/env python3
import sys
import os
import time
import logging
from ari_manager import ARI

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Entorno Asterisk / Dialer
ASTERISK_USER = os.getenv('ASTERISK_USER', 'default_user')
ASTERISK_PASS = os.getenv('ASTERISK_PASS', 'default_pass')
ASTERISK_HOST = os.getenv('ASTERISK_HOST', 'dialer_acd')
ASTERISK_PORT = int(os.getenv('ASTERISK_PORT', '8888'))
ASTERISK_APP = os.getenv('ASTERISK_APP', 'dialer_dialplan')

# Host destino para el endpoint PJSIP/<num>@<host> (equivale a DIALER_ACD_HOST en tu worker)
DIALER_ACD_HOST = os.getenv('DIALER_ACD_HOST', 'dialer_acd')

# (Opcional) Prefijo de salida para simular get_prefix(id_campaign) en tu worker.
OUTBOUND_PREFIX = os.getenv('OUTBOUND_PREFIX', '').strip()

USO = ("Uso: python call_sender.py <NUMERO> <ID_CAMP> <ID_CUSTOMER> "
       "<QUEUE_TIMEOUT> <DIAL_TIMEOUT> <CALL_TYPE>")

# Validación CLI
if len(sys.argv) != 7:
    logging.error(USO)
    sys.exit(1)

tel_number = sys.argv[1].strip()
id_camp = sys.argv[2].strip()
id_customer = sys.argv[3].strip()
queue_timeout = sys.argv[4].strip()   # Será forzado a 20
dial_timeout = sys.argv[5].strip()
call_type_in = sys.argv[6].strip()   # Será forzado a "2"

# Validación de entradas
if not tel_number.isdigit() or len(tel_number) < 7:
    logging.error(f'Número de teléfono inválido: {tel_number}')
    sys.exit(1)

if not dial_timeout.isdigit() or int(dial_timeout) <= 0:
    logging.error(f'Tiempo de marcación inválido: {dial_timeout}')
    sys.exit(1)

# Forzar valores para replicar attempt_contact_asterisk
FORCED_QUEUE_TIMEOUT = 20
FORCED_CALL_TYPE = '2'
if queue_timeout != str(FORCED_QUEUE_TIMEOUT):
    logging.info(
        f"Se ignora queue_timeout={queue_timeout} y se fuerza a "
        f"{FORCED_QUEUE_TIMEOUT} para emular el worker."
    )
if call_type_in != FORCED_CALL_TYPE:
    logging.info(
        f"Se ignora call_type={call_type_in} y se fuerza a "
        f"{FORCED_CALL_TYPE} (saliente a OML ACD)."
    )

# Aplicar prefijo si viene por env (simula get_prefix)
phone_to_dial = f"{OUTBOUND_PREFIX}{tel_number}" if OUTBOUND_PREFIX else tel_number

# callid como en el worker: "<epoch>.<id_customer>"
epoch = int(time.time())
callid = f"{epoch}.{id_customer}"

channel_type = 'to_omlacd_dialout'
caller_id = f'{id_camp}_{id_customer}_{phone_to_dial}'
endpoint = f'PJSIP/{phone_to_dial}@pstn_gateway'
variables = {
    'PJSIP_HEADER(add,OMLCODCLI)': f'{id_customer}',
    'PJSIP_HEADER(add,OMLCAMPID)': f'{id_camp}',
    'PJSIP_HEADER(add,OMLOUTNUM)': f'{phone_to_dial}',
    'PJSIP_HEADER(add,OMLUNIQUEID)': f'{callid}',
}

# appArgs con TODOS los campos que usa el worker
appArgs = (
    f"id_camp: {id_camp}, id_customer: {id_customer}, "
    f"tel_customer: {phone_to_dial}, queue_timeout: {FORCED_QUEUE_TIMEOUT}, "
    f"channel_type: {channel_type}, call_type: {FORCED_CALL_TYPE}, uniqueid: {callid}"
)

logging.info(
    f"Generando llamada (worker-like) a {phone_to_dial} | camp={id_camp} "
    f"customer={id_customer} callid={callid} endpoint={endpoint}"
)

# Crear ARI
try:
    ari = ARI(user=ASTERISK_USER, password=ASTERISK_PASS, host=ASTERISK_HOST, port=ASTERISK_PORT)
except Exception as e:
    logging.error(f'Error al conectar con ARI: {e}')
    sys.exit(1)

# Originar canal
try:
    response = ari.originate_channel(
        endpoint=endpoint,
        app=ASTERISK_APP,
        callerId=caller_id,
        appArgs=appArgs,
        variables=variables,
        timeout=int(dial_timeout)  # el worker no lo pasa, pero aquí lo respetamos
    )
except Exception as e:
    logging.error(f'Error al intentar generar la llamada: {e}')
    sys.exit(1)

# Respuesta
if response and isinstance(response, dict) and 'id' in response:
    logging.info(f'Llamada generada exitosamente con ID de canal: {response["id"]}')
else:
    logging.error(f'Error al generar la llamada: {response}')
    sys.exit(1)
