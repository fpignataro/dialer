import os
import requests
from requests.exceptions import HTTPError
import logging


class ARI:

    def __init__(self, user=None, password=None, host=None, port=None):
        self.host = host if host is not None else os.getenv('ASTERISK_HOST', 'acd')
        self.port = port if port is not None else os.getenv('ASTERISK_PORT', '7088')
        self.user = user if user is not None else os.getenv('ASTERISK_USER', 'omnileads')
        self.password = password if password is not None else os.getenv('ASTERISK_PASS')

    def post(self, route, payload=None, headers=None):
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        logging.info(f"URI: {uri}, Payload: {payload}, Headers: {headers}")
        response = requests.post(uri, auth=(self.user, self.password),
                                 json=payload, headers=headers)

        if response.status_code == 204 or not response.text:
            logging.info(f"No content in response. Status Code: {response.status_code}")
            return None
        else:
            try:
                response_json = response.json()
                logging.info(f"Response: {response_json}")
                return response_json
            except ValueError:
                logging.error(f"Error parsing JSON: {response.text}, "
                              f"Status Code: {response.status_code}")
                return response

    def get(self, route):
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        response = requests.get(uri, auth=(self.user, self.password))
        try:
            return response.json()
        except ValueError:
            logging.error(f"GET Error parsing JSON: {response.text}, Status Code:"
                          f" {response.status_code}")
            return response

    def put(self, route, payload=None, headers=None):
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        logging.info(f"URI: {uri}, Payload: {payload}, Headers: {headers}")
        response = requests.put(uri, auth=(self.user, self.password), json=payload, headers=headers)

        if response.status_code == 204 or not response.text:
            logging.info(f"No content in response. Status Code: {response.status_code}")
            return None
        else:
            try:
                response_json = response.json()
                logging.info(f"Response: {response_json}")
                return response_json
            except ValueError:
                logging.error(f"Error parsing JSON: {response.text}, "
                              f"Status Code: {response.status_code}")
                return response

    def delete(self, route):
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        return requests.delete(uri, auth=(self.user, self.password))

    def playback(self, channel_id, sound):
        route = f'channels/{channel_id}/play?media=sound:{sound}'
        return self.post(route)

    def stop_playback(self, playback_id):
        route = f'playbacks/{playback_id}'
        return self.delete(route)

    def get_playback(self, playback_id):
        route = f'playbacks/{playback_id}'
        return self.get(route)

    def get_channel_details(self, channel_id):
        route = f'channels/{channel_id}'
        return self.get(route)

    def start_moh(self, channel_id, moh_class='default'):
        route = f'channels/{channel_id}/moh'
        if moh_class:
            route += f'?mohClass={moh_class}'
        return self.post(route)

    def stop_moh(self, channel_id):
        route = f'channels/{channel_id}/moh'
        return self.delete(route)

    def answer(self, channel_id):
        route = f'channels/{channel_id}/answer'
        return self.post(route)

    def continue_call(self, channel_id):
        route = f'channels/{channel_id}/continue'
        return self.post(route)

    def create_channel(self, channel):
        route = f'channels?endpoint={channel}&app=survey'
        return self.post(route)

    def add_channel_to_bridge(self, bridge_id, channel_id):
        route = f'bridges/{bridge_id}/addChannel'
        payload = {'channel': channel_id}
        return self.post(route, payload)

    def create_bridge(self, bridge_type='mixing'):
        route = 'bridges'
        payload = {'type': bridge_type}
        return self.post(route, payload)

    def originate_channel(self, endpoint, app, callerId=None, appArgs=None, variables=None,
                          timeout=30, channelId=None):
        """
        Realiza una solicitud para crear un nuevo canal en Asterisk.
        """
        route = 'channels'
        payload = {
            'endpoint': endpoint,
            'app': app,
            'timeout': timeout  # Agrega el timeout al payload
        }

        # Añadir callerId al payload si se proporciona
        if callerId is not None:
            payload['callerId'] = callerId

        # Añadir appArgs al payload si se proporciona
        if appArgs is not None:
            payload['appArgs'] = appArgs

        # Añadir variables al payload si se proporcionan
        if variables is not None:
            payload['variables'] = variables

        # Añadir channelId al payload si se proporciona
        if channelId is not None:
            payload['channelId'] = channelId

        # Realizar la solicitud POST con el payload
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        try:
            response = requests.post(uri, auth=(self.user, self.password),
                                     json=payload, headers={'Content-Type': 'application/json'})
            # Verificar si la respuesta es exitosa
            if response.status_code == 200:
                response_json = response.json()
                logging.info(f'Response: {response_json}')
                return response_json
            else:
                # La respuesta no es exitosa, loguea el error y devuelve None
                logging.error(f"Error al realizar la solicitud ARI: {response.status_code}"
                              f" - {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Error al realizar la solicitud ARI: {str(e)}")
            return None  # Retorna None si ocurre un error

    def hangup_channel(self, channel_id):
        route = f'channels/{channel_id}'
        try:
            response = self.delete(route)
            if response.status_code == 204:
                logging.info(f"Channel {channel_id} hung up successfully.")
                return True
            else:
                logging.error(f"Failed to hang up channel {channel_id}: {response.status_code}")
                return False
        except HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')
            return False
        except Exception as err:
            logging.error(f'An error occurred: {err}')
            return False

    def get_channels_in_bridge(self, bridge_id):
        route = f'bridges/{bridge_id}'
        response = self.get(route)

        if isinstance(response, dict):
            return response.get('channels', [])
        else:
            if response.status_code == 200:
                try:
                    bridge_data = response.json()
                    return bridge_data.get('channels', [])
                except ValueError:
                    logging.error(f"Error parsing response JSON for bridge {bridge_id}")
                    return []
            else:
                logging.error(f"Failed to retrieve channels for bridge {bridge_id}: "
                              f"{response.status_code}")
                return []

    def destroy_bridge(self, bridge_id):
        route = f'bridges/{bridge_id}'
        response = self.delete(route)
        if response.status_code == 204:
            logging.info(f"Bridge {bridge_id} destroyed successfully.")
            return True
        else:
            logging.error(f"Failed to destroy bridge {bridge_id}: {response.status_code}")
            return False

    def get_channel_variable(self, channel_id, variable_name):
        try:
            route = f'channels/{channel_id}/variable?variable={variable_name}'
            response = self.get(route)

            if isinstance(response, dict):
                return response.get('value')
            else:
                logging.error("Error al obtener la variable del canal: respuesta inesperada")
                return None
        except Exception as e:
            logging.error(f"Error al obtener la variable del canal: {e}")
            return None

    def start_channel_recording(self, channel_id, name, format, maxDurationSeconds=0,
                                maxSilenceSeconds=0,
                                ifExists='fail', beep=False, terminateOn='none'):
        route = f'channels/{channel_id}/record'
        payload = {
            'name': name,
            'format': format,
            'maxDurationSeconds': str(maxDurationSeconds),
            'maxSilenceSeconds': str(maxSilenceSeconds),
            'ifExists': ifExists,
            'beep': beep,
            'terminateOn': terminateOn
        }
        return self.post(route, payload=payload)

    def start_recording(self, bridge_id, name, format, maxDurationSeconds=0, maxSilenceSeconds=0,
                        ifExists='fail', beep=False, terminateOn='none'):
        route = f'bridges/{bridge_id}/record'
        payload = {
            'name': name,
            'format': format,
            'maxDurationSeconds': maxDurationSeconds,
            'maxSilenceSeconds': maxSilenceSeconds,
            'ifExists': ifExists,
            'beep': beep,
            'terminateOn': terminateOn
        }

        try:
            response = self.post(route, payload)
            if response is None:
                logging.warning("No content in response to start_recording. Check bridge state.")
            return response
        except HTTPError as http_err:
            logging.error(f'HTTP error occurred in start_recording: {http_err}')
            if http_err.response.status_code == 404:
                logging.error("Bridge not found.")
            elif http_err.response.status_code == 409:
                logging.error("Bridge is not in a Stasis application or a recording with the same "
                              "name already exists.")
            elif http_err.response.status_code == 422:
                logging.error("The format specified is unknown on this system.")
            return None
        except Exception as e:
            logging.error(f'An error occurred in start_recording: {e}')
            return None

    def external_media(self, external_host, external_port, app, format='slin16', direction='both',
                       variables=None):
        route = 'channels/externalMedia'
        payload = {
            "external_host": f"{external_host}:{external_port}",
            "app": app,
            "format": format,
            "direction": direction
        }

        if variables is not None:
            payload['variables'] = variables

        return self.post(route, payload=payload)

    def continue_channel(self, channel_id, context, extension, priority):
        route = f'channels/{channel_id}/continue'
        payload = {
            'context': context,
            'extension': extension,
            'priority': priority
        }
        response = self.post(route, payload)

        if response is None or (hasattr(response, 'status_code') and response.status_code == 204):
            logging.info(f"Channel {channel_id} continued successfully in the dialplan.")
            return True
        else:
            logging.error(f"Failed to continue channel {channel_id}: {response.status_code} - "
                          f"{response.text}")
            return False

    def execute_asterisk_command(self, command):
        route = 'asterisk/execute'
        payload = {'command': command}
        response = self.post(route, payload)

        if response and 'response' in response:
            return response['response']
        else:
            logging.error(f"Unexpected response structure: {response}")
            return None

    def reload_module(self, module_name):
        route = f'asterisk/modules/{module_name}'
        response = self.put(route)

        if response is None or (hasattr(response, 'status_code') and response.status_code == 204):
            logging.info(f"Module {module_name} reloaded successfully.")
            return True
        elif response.status_code == 404:
            logging.error(f"Module {module_name} not found.")
            return False
        elif response.status_code == 409:
            logging.error(f"Module {module_name} could not be reloaded.")
            return False
        else:
            logging.error(f"Failed to reload module {module_name}: {response.status_code} - "
                          f"{response.text}")
            return False
