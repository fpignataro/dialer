import sys
import os
import logging
from ari_manager import ARI

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ASTERISK_USER = os.getenv('ASTERISK_USER', 'default_user')
ASTERISK_PASS = os.getenv('ASTERISK_PASS', 'default_pass')
ASTERISK_HOST = os.getenv('ASTERISK_HOST', 'dialer_acd')
ASTERISK_PORT = os.getenv('ASTERISK_PORT', '8888')
ASTERISK_APP = os.getenv('ASTERISK_APP', 'call_manager')
PSTN_GATEWAY = os.getenv('PSTN_GW', 'pstn_gateway')

# Verifica que se hayan proporcionado los argumentos necesarios
if len(sys.argv) != 7:
    logging.error("Uso: python call_sender.py <NUMERO> <ID_CAMP> <ID_CUSTOMER> <QUEUE_TIMEOUT> "
                  "<DIAL_TIMEOUT> <CALL_TYPE>")
    sys.exit(1)

# Obtiene los datos de los argumentos de la línea de comandos
tel_number = sys.argv[1]
id_camp = sys.argv[2]
id_customer = sys.argv[3]
queue_timeout = sys.argv[4]
dial_timeout = sys.argv[5]
call_type = sys.argv[6]
channel_type = 'to_external'

# Validación de entradas
if not tel_number.isdigit() or len(tel_number) < 7:
    logging.error(f'Número de teléfono inválido: {tel_number}')
    sys.exit(1)

if not queue_timeout.isdigit() or int(queue_timeout) <= 0:
    logging.error(f'Tiempo de espera en cola inválido: {queue_timeout}')
    sys.exit(1)

if not dial_timeout.isdigit() or int(dial_timeout) <= 0:
    logging.error(f'Tiempo de marcación inválido: {dial_timeout}')
    sys.exit(1)

if call_type not in ['1', '2']:
    logging.error(f'Tipo de llamada inválido: {call_type}')
    sys.exit(1)

# Crear una instancia de la clase ARI
try:
    ari = ARI(
        user=ASTERISK_USER,
        password=ASTERISK_PASS,
        host=ASTERISK_HOST,
        port=int(ASTERISK_PORT)
    )
except Exception as e:
    logging.error(f'Error al conectar con ARI: {e}')
    sys.exit(1)

# Dependiendo del call_type se llama a externo o a Dialer
if call_type == '1':
    endpoint = f'PJSIP/{queue_timeout}'
    caller_id = tel_number
    channel_type = 'to_agent'
    variables = {
        'PJSIP_HEADER(add,OMLCODCLI)': f'{id_customer}',
        'PJSIP_HEADER(add,OMLCAMPID)': f'{id_camp}',
        'PJSIP_HEADER(add,OMLOUTNUM)': f'{tel_number}',
    }
    logging.info(f'Generando llamada de agente a {tel_number}')
elif call_type == '2':
    endpoint = f'PJSIP/{tel_number}@pstn_gateway'
    caller_id = f'{id_camp}_{id_customer}_{tel_number}'
    channel_type = 'to_omlacd_dialout'
    variables = {
        'PJSIP_HEADER(add,OMLCODCLI)': f'{id_customer}',
        'PJSIP_HEADER(add,OMLCAMPID)': f'{id_camp}',
        'PJSIP_HEADER(add,OMLOUTNUM)': f'{tel_number}',
    }
    logging.info(f'Generando llamada saliente a {tel_number}')

call_data = {
    'endpoint': endpoint,
    'callerId': caller_id,
    'timeout': int(dial_timeout),
    'app': ASTERISK_APP,
    'appArgs': (f'id_camp: {id_camp}, id_customer: {id_customer}, tel_customer: {tel_number}, '
                f'channel_type: {channel_type}, call_type: {call_type}'),
    'variables': variables,
}

# Incluye el call_session_id como parte de las variables de la llamada
try:
    response = ari.originate_channel(
        endpoint=call_data['endpoint'],
        app=call_data['app'],
        callerId=call_data['callerId'],
        appArgs=call_data['appArgs'],
        variables=call_data['variables'],
        timeout=call_data['timeout'],
    )
except Exception as e:
    logging.error(f'Error al intentar generar la llamada: {e}')
    sys.exit(1)

# Verifica la respuesta de Asterisk
if response and isinstance(response, dict) and 'id' in response:
    logging.info(f'Llamada generada exitosamente con ID de canal: {response["id"]}')
else:
    logging.error(f'Error al generar la llamada: {response}')
    sys.exit(1)
