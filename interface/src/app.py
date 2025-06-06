# -*- coding: utf-8 -*-

from flask import Flask, request, render_template

from dialer.multichannel import GearmanDialer

from settings.default import WEBSOCKET_SERVER


app = Flask(__name__)

DIALER = GearmanDialer

# TODO: pass a parameter called 'type' for dispatch to
# the class linked to that kind of a campaing (voip, email, Whatsapp, Telegram, SMS, etc)


@app.route('/create-campaign/<id_campaign>', methods=['POST'])
def create_campaign(id_campaign):
    strategy = request.get_json().get('contact-strategy', [])
    prefix = request.get_json().get('prefix', [])
    return DIALER.create_campaign(id_campaign, strategy, prefix)


@app.route('/edit-campaign/<id_campaign>', methods=['POST'])
def edit_campaign(id_campaign):
    strategy = request.get_json().get('contact-strategy', [])
    return DIALER.edit_campaign(id_campaign, strategy)


@app.route('/start-campaign/<id_campaign>', methods=['POST'])
def start_campaign(id_campaign):
    sync_omnileads = request.form.get('sync-omnileads', False) and True
    return DIALER.start_campaign(id_campaign, sync_omnileads=sync_omnileads)


@app.route('/stop-campaign/<id_campaign>', methods=['POST'])
def stop_campaign(id_campaign):
    sync_omnileads = request.form.get('sync-omnileads', False) and True
    return DIALER.stop_campaign(id_campaign, sync_omnileads=sync_omnileads)


@app.route('/pause-campaign/<id_campaign>', methods=['POST'])
def pause_campaign(id_campaign):
    sync_omnileads = request.form.get('sync-omnileads', False) and True
    return DIALER.pause_campaign(id_campaign, sync_omnileads=sync_omnileads)


@app.route('/resume-campaign/<id_campaign>', methods=['POST'])
def resume_campaign(id_campaign):
    return DIALER.resume_campaign(id_campaign)


@app.route('/delete-campaign/<id_campaign>', methods=['POST'])
def delete_campaign(id_campaign):
    return DIALER.delete_campaign(id_campaign)


@app.route('/add-incidence-rule-disposition/<id_campaign>', methods=['POST'])
def add_incidence_rule_disposition(id_campaign):
    id_contact = request.get_json().get('id_contact', -1)
    disposition_option = request.get_json().get('disposition_option', -1)
    return DIALER.add_incidence_rule_disposition(id_campaign, disposition_option, id_contact)


@app.route('/create-incidence-rule/<id_campaign>', methods=['POST'])
def create_incidence_rule(id_campaign):
    json_value = request.get_json()
    id_rule = json_value.get('id', -1)
    type_rule = json_value.get('type', -1)
    status = json_value.get('status', -1)
    status_custom = json_value.get('status_custom', "")
    disposition_option_id = json_value.get('disposition_option_id', -1)
    max_attempt = json_value.get('max_attempt', -1)
    retry_later = json_value.get('retry_later', -1)
    mode = json_value.get('in_mode', -1)
    return DIALER.create_incidence_rule(
        id_campaign, id_rule, status, status_custom, max_attempt,
        retry_later, mode, disposition_option_id, type_rule
    )


@app.route('/delete-incidence-rule/<id_campaign>', methods=['POST'])
def delete_incidence_rule(id_campaign):
    json_value = request.get_json()
    id_rule = json_value.get('id')
    type_rule = json_value.get('type')
    return DIALER.delete_incidence_rule(
        id_campaign, id_rule, type_rule
    )


@app.route('/update-incidence-rule/<id_campaign>', methods=['POST'])
def update_incidence_rule(id_campaign):
    json_value = request.get_json()
    id_rule = json_value.get('id', -1)
    type_rule = json_value.get('type', -1)
    status = json_value.get('status', -1)
    status_custom = json_value.get('status_custom', "")
    disposition_option_id = json_value.get('disposition_option_id', -1)
    max_attempt = json_value.get('max_attempt', -1)
    retry_later = json_value.get('retry_later', -1)
    mode = json_value.get('in_mode', -1)
    return DIALER.update_incidence_rule(
        id_campaign, id_rule, status, status_custom, max_attempt,
        retry_later, mode, disposition_option_id, type_rule
    )


@app.route('/add-agenda/<id_campaign>', methods=['POST'])
def add_agenda(id_campaign):
    datetime_agenda = request.get_json().get('datetime', '')
    campaign_name = request.get_json().get('campaign_name', '')
    phone_number = request.get_json().get('phone_number', '')
    id_contact = request.get_json().get('id_contact', '')
    return DIALER.add_agenda(id_campaign, id_contact, campaign_name, datetime_agenda, phone_number)


@app.route('/change-database/<id_campaign>', methods=['POST'])
def change_database(id_campaign):
    return DIALER.change_database(id_campaign)


# HTMX endpoints & UI related code

app.jinja_env.globals['WEBSOCKET_SERVER'] = WEBSOCKET_SERVER


# TODO: move this endpoint to the workers
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/htmx/init')
def init():
    """Renders the initial data of the campaigns, on the first load of the web page"""
    return DIALER.render_template({'type': 'init'})


@app.route('/htmx/stats/<id_campaign>')
def stats(id_campaign):
    """Renders the initial data of the campaigns, on the first load of the web page"""
    return DIALER.render_template(
        {'type': 'stats', 'id_campaign': id_campaign}
    )


@app.route('/htmx/manage-dialer/', methods=['POST'])
def manage_dialer():
    action = request.form.get('action')
    return DIALER.manage_dialer(action)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=1440, debug=True)
