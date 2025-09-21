import re
import time
import json
import logging
import requests
import os
import gearman
import signal
import sys
import traceback
import websocket
import redis
from ari_manager import ARI
from gearman.job import JOB_CREATED
from gearman.errors import ServerUnavailable, ConnectionError

PYTHON_LOGLEVEL = os.environ.get("PYTHON_LOGLEVEL", "info")

if PYTHON_LOGLEVEL.lower() == "debug":
    class JsonFormatter(logging.Formatter):
        def format(self, record):
            log_record = {
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(record.created)),
                "level": record.levelname,
                "message": record.getMessage(),
            }
            return json.dumps(log_record)

    json_handler = logging.StreamHandler(sys.stdout)
    json_handler.setFormatter(JsonFormatter())

    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[json_handler]
    )
else:
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


class CallManager:
    def __init__(self):
        self.ari = ARI()
        self.calls = {}
        self.ws = None
        self.shutting_down = False

        # Init redis cli
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_DIALER_SERVER', 'localhost'),
            port=int(os.getenv('REDIS_DIALER_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 0))
        )

        self.pstn_channel_ids = set()
        self.agent_to_pstn = {}
        self.channel_dialstatus = {}

        self.gearman_task = os.getenv('GEARMAN_TASK', 'call_log_processor')
        gm_host = os.getenv('GEARMAN_JOB_SERVERS', 'localhost:4730')
        self.gearman_hostport = gm_host
        self.gearman_client = gearman.GearmanClient([gm_host])
        logging.info("Conectado a Gearman en %s", gm_host)
        self.pstngw_hostname = os.getenv("PSTNGW_HOSTNAME")

    def client(self):
        try:
            ari_client = websocket.WebSocketApp(
                f"ws://{self.ari.host}:{self.ari.port}/ari/events"
                f"?api_key={self.ari.user}:{self.ari.password}&app={ASTERISK_APP}",
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            return ari_client
        except Exception as e:
            logging.error("Error setting up WS ARI client: %s", str(e))
            logging.error(traceback.format_exc())
            return None

    def on_message(self, ws, message):

        event_to_dict = json.loads(message)

        if "RTP" not in message and "ChannelVarset" not in message:
            logger = logging.getLogger()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "\n" + "=" * 50 + "\nJSON event from WS:\n%s\n%s",
                    json.dumps(event_to_dict, indent=2),
                    "=" * 50
                )
            else:
                logger.info(
                    "JSON event from WS: %s",
                    event_to_dict.get("type", "Unknown")
                )

        if event_to_dict.get("type") == "Dial":
            peer = event_to_dict.get("peer")
            if peer:
                caller = peer.get("caller", {})
                caller_name = caller.get("name", "")
                channel_id = peer.get("id", "Unknown")
                logging.info("Peer ID: %s", channel_id)
                if not caller_name:
                    logging.warning("Caller 'name' is empty for channel ID %s", channel_id)
        else:
            logging.info("Non-Dial Event: %s", event_to_dict.get("type"))

        event_handlers = {
            'StasisStart': self.handle_stasis_start,
            'StasisEnd': self.handle_stasis_end,
            'ChannelDtmfReceived': self.handle_channel_dtmf_received,
            'ChannelHangupRequest': self.handle_channel_hangup_request,
            'ChannelStateChange': self.handle_channel_state_change,
            'ChannelCreated': self.handle_channel_created,
            'ChannelDestroyed': self.handle_channel_destroyed,
            'BridgeCreated': self.handle_bridge_created,
            'BridgeDestroyed': self.handle_bridge_destroyed,
            'PlaybackStarted': self.handle_playback_started,
            'PlaybackFinished': self.handle_playback_finished,
            'Dial': self.handle_dial,
        }

        event_type = event_to_dict.get('type', 'default')

        handler = event_handlers.get(event_type)
        if handler:
            handler(event_to_dict)

    def on_error(self, ws, error):
        logging.error("WebSocket Error: %s", error)

    def on_close(self, ws, close_status_code, close_msg):
        logging.info("WebSocket closed connection")
        # The reconnection logic is now handled by the loop in start_websocket.

    def on_open(self, ws):
        logging.info("WebSocket connection opened")

    def reconnect(self):
        logging.info("Attempting to reconnect in 10 seconds...")
        time.sleep(10)
        # The recursive call to start_websocket() is removed to prevent stack overflow.
        # The main loop in start_websocket will handle reconnection.
        logging.warning("reconnect() called, but it's part of an old and problematic reconnect logic.")

    def start_websocket(self):
        def signal_handler(signum, frame):
            logging.info("Signal received, shutting down...")
            self.shutting_down = True
            if self.ws:
                self.ws.close()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        while not self.shutting_down:
            self.ws = self.client()
            if self.ws is None:
                logging.error("Unable to create WebSocket client instance. Retrying in 10 seconds.")
                time.sleep(10)
                continue

            self.ws.on_open = self.on_open
            self.ws.on_message = self.on_message
            self.ws.on_error = self.on_error
            self.ws.on_close = self.on_close

            self.ws.run_forever()

            if not self.shutting_down:
                logging.info("WebSocket connection lost. Attempting to reconnect in 10 seconds...")
                time.sleep(10)

        self.shutdown()

    def shutdown(self):
        # Cerramos conexión con RabbitMQ
        sys.exit(0)

    def handle_dial(self, event):
        try:
            dialstatus = event.get('dialstatus')
            dialstring = event.get('dialstring', 'Unknown')
            peer = event.get('peer', {})
            peer_id = peer.get('id')
            channel = event.get('channel', {})
            channel_id = channel.get('id')

            # Obtener caller_name del peer
            caller = peer.get('caller', {})
            caller_name = caller.get('name', '')

            # Parsear caller_name para obtener id_camp, id_customer, tel_customer
            id_camp, id_customer, tel_customer = self.parse_caller_name(caller_name)

            call_data = None

            # Guardar el dialstatus asociado al canal correcto para usarlo en
            # el evento ChannelDestroyed. Para llamadas PSTN el identificador
            # relevante es el peer_id; en otros casos usamos el channel_id.
            dialstatus_key = None
            if re.match(r'^\d+@pstn_gateway$', dialstring):
                dialstatus_key = peer_id or channel_id
            else:
                dialstatus_key = channel_id or peer_id

            if dialstatus_key:
                status_to_store = dialstatus if dialstatus else 'UNKNOWN'
                if dialstatus or dialstatus_key not in self.channel_dialstatus:
                    self.channel_dialstatus[dialstatus_key] = status_to_store
                logging.debug(
                    "Stored dialstatus '%s' for channel %s", status_to_store,
                    dialstatus_key
                )

            # Verificar si es una llamada DIALER-PSTN
            if re.match(r'^\d+@pstn_gateway$', dialstring):
                if dialstatus == '':
                    if id_camp:
                        redis_key = f"OML:CALLS:{id_camp}:DIALER"
                        self.redis_client.incr(redis_key)
                        logging.info(f"Incremented calls counter in Redis key '{redis_key}'")
                    else:
                        logging.warning("id_camp not defined. Calls counter not incremented.")

                    if peer_id:
                        self.pstn_channel_ids.add(peer_id)
                        logging.info(f"Added PSTN channel id {peer_id} to tracking set")

                    if self.pstngw_hostname and id_camp and id_customer and tel_customer:
                        call_data = {
                            'id_camp': id_camp,
                            'id_customer': id_customer,
                            'tel_customer': tel_customer,
                            'channel_id': peer_id or channel_id
                        }
                        self.publish_message_if_needed(call_data, "DIAL")
                    else:
                        logging.warning("PSTNGW_HOSTNAME not set or no data extracted from caller "
                                        "name. Message not published.")

                elif dialstatus == 'ANSWER':
                    if self.pstngw_hostname and id_camp and id_customer and tel_customer:
                        call_data = {
                            'id_camp': id_camp,
                            'id_customer': id_customer,
                            'tel_customer': tel_customer,
                            'channel_id': peer_id or channel_id
                        }
                        # Publicar el evento ANSWER al RabbitMQ
                        self.publish_message_if_needed(call_data, "ANSWER")
                    else:
                        logging.warning("PSTNGW_HOSTNAME not set or no data extracted from caller "
                                        "name for ANSWER event. Message not published.")

                else:
                    # Otros dialstatus (RINGING, BUSY, CONGESTION, NOANSWER, etc.)
                    logging.info("Dial status: %s", dialstatus)
                    if self.pstngw_hostname and id_camp and id_customer and tel_customer:
                        call_data = {
                            'id_camp': id_camp,
                            'id_customer': id_customer,
                            'tel_customer': tel_customer,
                            'channel_id': peer_id or channel_id
                        }
                        # Publicar el evento ANSWER al RabbitMQ
                        self.publish_message_if_needed(call_data, dialstatus)
                    else:
                        logging.warning("PSTNGW_HOSTNAME not set or no data extracted from caller "
                                        "name for %s event. Message not published.",
                                        dialstatus or 'UNKNOWN')

            elif re.match(r'^camp_\d+@omlacd$', dialstring):
                # Es una llamada DIALER-AGENT
                if dialstatus == 'NOANSWER':
                    agent_channel_id = peer_id
                    pstn_channel_id = self.agent_to_pstn.get(agent_channel_id)
                    if pstn_channel_id:
                        self.hangup_channel(pstn_channel_id)
                        logging.info(
                            f"Hung up PSTN channel {pstn_channel_id} due to agent no answer.")
                    else:
                        logging.warning(
                            f"No PSTN channel ID found for agent channel {agent_channel_id}")
                else:
                    logging.info("Dial status: %s", dialstatus)
            else:
                # Si el dialstring no coincide con ninguno de los patrones anteriores
                logging.info("Dial status: %s", dialstatus)

        except Exception as e:
            logging.error("Error handling dial event: %s", str(e))

    def handle_stasis_start(self, event):
        try:
            channel_data = self.extract_channel_data(event)
            if channel_data:
                channel_id = channel_data.get('channel_id')

                self.calls[channel_id] = channel_data
                logging.info(f"Call data stored for channel {channel_id}")

                logging.info(
                    "StasisStart SIP Channel was Attended:"
                    "id_camp=%s, id_customer=%s, tel_customer=%s,"
                    "channel_type: %s, pstn_id_channel=%s,"
                    "bridge_id=%s, channel_id=%s",
                    channel_data.get('id_camp'), channel_data.get('id_customer'),
                    channel_data.get('tel_customer'),
                    channel_data.get('channel_type'), channel_data.get('channel_id_pstn'),
                    channel_data.get('bridge_id'), channel_id
                )

                channel_type = channel_data.get('channel_type')

                if channel_type == 'to_omlacd_dialout':
                    self.handle_to_omlacd_dialout(channel_id)
                elif channel_type == 'to_omlacd_dialqueue':
                    if channel_data.get('channel_id_pstn') and channel_id:
                        self.agent_to_pstn[channel_id] = channel_data.get('channel_id_pstn')
                        logging.info(f"Mapped agent channel {channel_id} to PSTN channel "
                                     f"{channel_data.get('channel_id_pstn')}")
                    self.handle_to_omlacd_queue(channel_id)
                else:
                    logging.error("Unknown channel type: %s", channel_type)
            else:
                logging.error("Failed to extract channel data")
        except Exception as e:
            logging.error("Error handling stasis start: %s", str(e))

    def handle_channel_hangup_request(self, event):
        try:
            channel = event.get('channel', {})
            channel_id = channel.get('id')
            cause = event.get('cause')

            if cause is not None:
                logging.info("Channel %s hangup request with cause %s", channel_id, cause)
                call_data = self.find_call_data_by_channel_id(channel_id)
                if call_data:
                    self.hangup_channel(call_data.get('omlacd_channel_id'))
                    self.hangup_channel(call_data.get('channel_id_pstn'))
                    self.hangup_channel(call_data.get('channel_id'))
                    bridge_id = call_data.get('bridge_id')
                    if bridge_id:
                        self.delete_bridge(bridge_id)
                    else:
                        logging.warning("No bridge_id found for call_data")
                else:
                    logging.warning(f"No call data found for channel {channel_id}")
            else:
                logging.info("Channel %s hangup request received but no cause provided", channel_id)
        except Exception as e:
            logging.error("Error handling hangup request: %s", str(e))

    def handle_channel_destroyed(self, event):
        try:
            channel = event.get('channel', {})
            channel_id = channel.get('id')
            caller = channel.get('caller', {})
            caller_name = caller.get('name', '')

            id_camp, id_customer, tel_customer = self.parse_caller_name(caller_name)

            dialstatus = self.channel_dialstatus.get(channel_id)

            if not dialstatus:
                mapped_channel_id = self.agent_to_pstn.get(channel_id)
                if mapped_channel_id:
                    dialstatus = self.channel_dialstatus.get(mapped_channel_id)
                    if dialstatus:
                        logging.debug(
                            "Recovered dialstatus '%s' from mapped channel %s",
                            dialstatus, mapped_channel_id
                        )

            if not dialstatus:
                fallback_status = (
                    event.get('dialstatus') or
                    event.get('cause_txt') or
                    event.get('cause')
                )
                if fallback_status:
                    dialstatus = str(fallback_status)
                    logging.debug(
                        "Using fallback dialstatus '%s' for channel %s",
                        dialstatus, channel_id
                    )
                else:
                    dialstatus = "UNKNOWN"
                    
            if channel_id in self.pstn_channel_ids:
                if id_camp:
                    redis_key = f"OML:CALLS:{id_camp}:DIALER"
                    self.redis_client.decr(redis_key)
                    logging.info(f"Decremented calls counter in Redis key '{redis_key}' for channel"
                                 f" id {channel_id}")
                else:
                    logging.warning("id_camp not defined. Calls counter not decremented.")

                self.pstn_channel_ids.remove(channel_id)

                if self.pstngw_hostname and id_camp and id_customer and tel_customer and dialstatus:
                    if dialstatus != 'ANSWER':
                        call_data = {
                            'id_camp': id_camp,
                            'id_customer': id_customer,
                            'tel_customer': tel_customer,
                            'channel_id': channel_id,
                            'dialstatus': dialstatus
                        }
                        self.publish_message_if_needed(call_data, dialstatus)
                    else:
                        logging.info(f"Dialstatus is ANSWER, not publishing to RabbitMQ for "
                                     f"channel {channel_id}.")
                else:
                    logging.warning(f"No call data extracted or PSTNGW_HOSTNAME not set or no "
                                    f"dialstatus. Channel {channel_id}. Message not published.")
            else:
                logging.info(f"ChannelDestroyed for channel id {channel_id}, not a tracked "
                             f"PSTN channel")

            # Remover el dialstatus guardado para liberar memoria
            if channel_id in self.channel_dialstatus:
                del self.channel_dialstatus[channel_id]

        except Exception as e:
            logging.error("Error handling ChannelDestroyed event: %s", str(e))

    def handle_channel_dtmf_received(self, event):
        channel = event.get('channel', {})
        channel_id = channel.get('id')
        digit = event.get("digit")
        logging.info("DTMF recibido en el canal %s: %s", channel_id, digit)

    def handle_stasis_end(self, event):
        try:
            channel = event.get('channel', {})
            channel_id = channel.get('id')
            call_data = self.calls.pop(channel_id, None)

            if call_data:
                logging.info(
                    "StasisEnd to-omlacd SIP Channel hangup:"
                    "id_camp=%s, id_customer=%s, tel_customer=%s,"
                    "channel_type: %s, pstn_id_channel=%s,"
                    "id_bridge=%s, channel_id=%s",
                    call_data.get('id_camp'), call_data.get('id_customer'),
                    call_data.get('tel_customer'),
                    call_data.get('channel_type'), call_data.get('channel_id_pstn'),
                    call_data.get('bridge_id'), channel_id)

                if call_data.get('channel_type') == 'to_omlacd_dialout':
                    logging.info("***HANGUP to-omlacd DialOUT Channel***")
                    self.hangup_channel(channel_id)
                    self.hangup_channel(call_data.get('omlacd_channel_id'))
                    self.delete_bridge(call_data.get('bridge_id'))
                elif call_data.get('channel_type') == 'to_omlacd_dialqueue':
                    logging.info("***HANGUP to-omlacd DialQUEUE Channel***")
                    self.hangup_channel(call_data.get('omlacd_channel_id'))
                    self.hangup_channel(call_data.get('channel_id_pstn'))
                    self.delete_bridge(call_data.get('bridge_id'))
                else:
                    logging.error("StasisEnd FAIL channel_type ERROR")
            else:
                logging.warning(f"No call data found for channel {channel_id}")
        except Exception as e:
            logging.error("Error handling stasis end: %s", str(e))

    def handle_channel_state_change(self, event):
        try:
            channel = event.get('channel', {})
            channel_id = channel.get('id')
            channel_state = channel.get('state')
            logging.info("Channel %s state changed to %s", channel_id, channel_state)
            if channel_state == 'Ringing':
                logging.info("Channel %s is ringing", channel_id)
            elif channel_state == 'Busy':
                logging.info("Channel %s is busy", channel_id)
            elif channel_state == 'Up':
                logging.info("Channel %s is up", channel_id)
            elif channel_state == 'Down':
                logging.info("Channel %s is down", channel_id)
        except Exception as e:
            logging.error("Error handling channel state change: %s", str(e))

    def find_call_data_by_channel_id(self, channel_id):
        for cd in self.calls.values():
            if channel_id in [cd.get('channel_id'), cd.get('channel_id_pstn'),
                              cd.get('omlacd_channel_id')]:
                return cd
        return None

    def extract_channel_data(self, event):
        try:
            channel = event.get('channel', {})
            channel_id = channel.get('id')
            dialplan = channel.get('dialplan', {})
            app_data = dialplan.get('app_data', '')

            # Variables iniciales
            id_camp = None
            id_customer = None
            channel_type = None
            queue_timeout = None
            channel_id_pstn = None
            bridge_id = None
            phone_number = None
            id_agent = None
            call_type = None
            uniqueid = None  # NUEVA VARIABLE

            args = app_data.split(',')

            for arg in args:
                key_value = arg.split(':', 1)
                if len(key_value) == 2:
                    key, value = key_value
                    key = key.strip()
                    value = value.strip()

                    if key == 'id_camp':
                        id_camp = value
                    elif key == 'id_customer':
                        id_customer = value
                    elif key == 'tel_customer':
                        phone_number = value
                    elif key == 'queue_timeout':
                        queue_timeout = value
                    elif key == 'channel_type':
                        channel_type = value
                    elif key in ('call_type', 'id_calltype'):
                        try:
                            call_type = int(value)
                        except ValueError:
                            call_type = None
                    elif key == 'channel_id_pstn':
                        channel_id_pstn = value
                    elif key in ('bridge_id', 'id_bridge'):
                        bridge_id = value
                    elif key == 'id_agent':
                        id_agent = value
                    elif key == 'uniqueid':  # NUEVO PARSEO
                        uniqueid = value

            logging.info("Extracted channel data: %s", {
                'channel_id': channel_id,
                'id_camp': id_camp,
                'id_customer': id_customer,
                'tel_customer': phone_number,
                'queue_timeout': queue_timeout,
                'channel_type': channel_type,
                'channel_id_pstn': channel_id_pstn,
                'bridge_id': bridge_id,
                'id_agent': id_agent,
                'call_type': call_type,
                'uniqueid': uniqueid,  # LOG NUEVO
            })

            return {
                'channel_id': channel_id,
                'id_camp': id_camp,
                'id_customer': id_customer,
                'tel_customer': phone_number,
                'queue_timeout': queue_timeout,
                'channel_type': channel_type,
                'channel_id_pstn': channel_id_pstn,
                'bridge_id': bridge_id,
                'id_agent': id_agent,
                'call_type': call_type,
                'uniqueid': uniqueid,  # DEVOLUCIÓN NUEVA
            }

        except Exception as e:
            logging.error("Error extracting channel data: %s", str(e))
            return None

    def hangup_channel(self, channel_id):
        try:
            if channel_id:
                self.ari.hangup_channel(channel_id)
                logging.info("Channel %s colgado", channel_id)
            else:
                logging.warning("No channel ID provided to hangup_channel")
        except Exception as e:
            logging.error("Error al colgar el canal %s: %s", channel_id, str(e))

    def handle_to_omlacd_dialout(self, channel_id):
        logging.info("****** External Outbound Channel Start *****")

        call_data = self.calls.get(channel_id)
        if not call_data:
            logging.error(f"No call data found for channel {channel_id}")
            return

        bridge = self.ari.create_bridge()
        if bridge is not None and 'id' in bridge:
            bridge_id = bridge.get('id')
            call_data['bridge_id'] = bridge_id
            self.calls[channel_id] = call_data
        else:
            logging.error("Failed to create bridge or 'id' not present")
            return

        self.ari.add_channel_to_bridge(bridge_id, channel_id)
        logging.info("Added channel %s to bridge %s", channel_id, bridge_id)
        self.dial_to_omlacd_queue(channel_id)

    def handle_to_omlacd_queue(self, channel_id):
        logging.info("****** External OMLACD QUEUE Channel Start *****")

        call_data = self.calls.get(channel_id)
        if not call_data:
            logging.error(f"No call data found for channel {channel_id}")
            return

        bridge_id = call_data.get('bridge_id')
        call_type = call_data.get('call_type')

        logging.info("Valor de call_type: %s  bridge_id: %s", call_type, bridge_id)

        if not bridge_id:
            logging.error("Bridge ID is missing. Cannot add channels to bridge")
            return

        self.ari.add_channel_to_bridge(bridge_id, channel_id)
        logging.info("Added channel %s to bridge %s", channel_id, bridge_id)

    def dial_to_omlacd_queue(self, channel_id):
        try:
            call_data = self.calls.get(channel_id)
            if not call_data:
                logging.error(f"No call data found for channel {channel_id}")
                return

            uniqueid = call_data.get("uniqueid", "UNKNOWN")  # fallback por si falta

            originate_data = {
                'endpoint': f'PJSIP/camp_{call_data["id_camp"]}@omlacd',
                'callerId': (
                    f'{call_data["id_camp"]}_{call_data["id_customer"]}_'
                    f'{call_data["tel_customer"]}'
                ),
                'timeout': 12,
                'app': ASTERISK_APP,
                'appArgs': (f'id_camp: {call_data["id_camp"]},'
                            f'id_customer: {call_data["id_customer"]},'
                            f'tel_customer: {call_data["tel_customer"]},'
                            f'channel_type: to_omlacd_dialqueue,'
                            f'id_calltype: 2,'
                            f'channel_id_pstn: {call_data["channel_id"]},'
                            f'bridge_id: {call_data["bridge_id"]},'
                            f'uniqueid: {uniqueid}'),
                'variables': {
                    'PJSIP_HEADER(add,Origin)': 'DIALER',
                    'PJSIP_HEADER(add,OMLCODCLI)': f'{call_data["id_customer"]}',
                    'PJSIP_HEADER(add,OMLCAMPID)': f'{call_data["id_camp"]}',
                    'PJSIP_HEADER(add,OMLOUTNUM)': f'{call_data["tel_customer"]}',
                    'PJSIP_HEADER(add,OMLUNIQUEID)': uniqueid,
                }
            }

            response = self.ari.originate_channel(
                endpoint=originate_data['endpoint'],
                app=originate_data['app'],
                callerId=originate_data['callerId'],
                appArgs=originate_data['appArgs'],
                variables=originate_data['variables']
            )

            if response and isinstance(response, dict) and 'id' in response:
                logging.info('Llamada generada exitosamente')
                omlacd_channel_id = response['id']
                self.agent_to_pstn[omlacd_channel_id] = call_data['channel_id']
                logging.info(
                    f"Mapped agent channel {omlacd_channel_id} to PSTN channel "
                    f"{call_data['channel_id']}"
                )
                call_data['omlacd_channel_id'] = omlacd_channel_id
                self.calls[channel_id] = call_data
            else:
                logging.error('Error al generar la llamada: %s', response)

        except KeyError as e:
            logging.error("KeyError: Missing key in call_data: %s", str(e))
            logging.error(traceback.format_exc())
        except requests.exceptions.RequestException as e:
            logging.error("RequestException: Failed to make request to ARI: %s", str(e))
            logging.error(traceback.format_exc())
        except Exception as e:
            logging.error("Unexpected error: %s", str(e))
            logging.error(traceback.format_exc())

    def handle_channel_created(self, event):
        logging.info("Handling ChannelCreated event:")

    def handle_bridge_created(self, event):
        logging.info("Handling BridgeCreated event:")

    def handle_bridge_destroyed(self, event):
        logging.info("Handling BridgeDestroyed event:")

    def handle_playback_started(self, event):
        logging.info("Handling PlaybackStarted event")

    def handle_playback_finished(self, event):
        logging.info("Handling PlaybackFinished event:")

    def delete_bridge(self, bridge_id):
        try:
            if bridge_id:
                self.ari.destroy_bridge(bridge_id)
                logging.info("Bridge %s eliminado", bridge_id)
            else:
                logging.warning("No bridge ID provided to delete_bridge")
        except Exception as e:
            logging.error("Error al eliminar el bridge %s: %s", bridge_id, str(e))

    def parse_caller_name(self, caller_name):
        if caller_name:
            parts = caller_name.split('_')
            if len(parts) == 3:
                return parts[0], parts[1], parts[2]
            else:
                logging.warning("Caller name doesn't follow expected pattern "
                                "id_camp_id_customer_tel_customer: %s", caller_name)
                return None, None, None
        else:
            logging.warning("Caller name is empty, cannot extract call data")
            return None, None, None

    def publish_message_if_needed(self, call_data, event_type):
        if not self.pstngw_hostname:
            logging.warning("PSTNGW_HOSTNAME not set. Message not published.")
            return

        allowed_dialstatuses = {
            "ANSWER", "CANCEL", "BUSY", "CONGESTION",
            "AMD", "NOANSWER", "DIAL"
        }
        if event_type not in allowed_dialstatuses:
            logging.info(
                "Dialstatus %s no permitido. Mensaje no publicado.", event_type)
            return

        msg = {
            'callid': call_data.get('channel_id'),
            'campana_id': call_data.get('id_camp'),
            'tipo_campana': '2',
            'tipo_llamada': '2',
            'agente_id': 'dialer-dialout',
            'event': event_type,
            'numero_marcado': call_data.get('tel_customer'),
            'contacto_id': call_data.get('id_customer'),
            'bridge_wait_time': '-1',
            'duracion_llamada': '-1',
            'archivo_grabacion': '-1',
            'agente_extra_id': '-1',
            'campana_extra_id': '-1',
            'numero_extra': '-1'
        }

        self.publish_to_gearman(msg)

    def publish_to_gearman(self, message: dict) -> bool:
        if not message:
            logging.error("Mensaje vacío, no se puede enviar a Gearman")
            return False

        try:
            payload = json.dumps(message).encode('utf-8')
        except (TypeError, ValueError) as e:
            logging.error("Error al serializar mensaje a JSON: %s", e)
            return False

        host_port = self.gearman_hostport

        def _send() -> bool:
            try:
                logging.info(
                    "Enviando job Gearman con tarea '%s'", self.gearman_task
                )
                job = self.gearman_client.submit_job(
                    self.gearman_task,
                    payload,
                    background=True
                )

                if job.state == JOB_CREATED:
                    logging.info(
                        "Mensaje enviado a Gearman: %s",
                        payload.decode()
                    )
                    return True
                else:
                    logging.error("Job no creado (state=%s)", job.state)
                    return False
            except Exception as e:
                logging.error("Error en submit_job: %s", e)
                return False

        try:
            return _send()
        except (ServerUnavailable, ConnectionError) as e:
            logging.warning(
                "Sesión con Gearman perdida (%s); reintento con un nuevo cliente…",
                str(e)
            )
            try:
                self.gearman_client = gearman.GearmanClient([host_port])
                return _send()
            except Exception as err:
                logging.error("Reconexión a Gearman fallida: %s", err)
                return False
        except Exception as err:
            logging.error("Error inesperado enviando a Gearman: %s", err)
            return False


if __name__ == "__main__":
    ASTERISK_USER = os.getenv('ASTERISK_USER', 'default_user')
    ASTERISK_PASS = os.getenv('ASTERISK_PASS', 'default_pass')
    ASTERISK_HOST = os.getenv('ASTERISK_HOST', 'dialer-acd')
    ASTERISK_PORT = os.getenv('ASTERISK_PORT', '8888')
    ASTERISK_APP = 'dialer_dialplan'

    ari_client = ARI(
        user=ASTERISK_USER,
        password=ASTERISK_PASS,
        host=ASTERISK_HOST,
        port=ASTERISK_PORT
    )

    call_manager = CallManager()
    call_manager.start_websocket()
