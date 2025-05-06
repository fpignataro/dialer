from settings.default import GEARMAN_JOB_SERVERS, GEARMAN_JOBS

from handler.naive import AverageWorker

import gearman


WORKER = AverageWorker

gm_worker = gearman.GearmanWorker(GEARMAN_JOB_SERVERS)


JOBS_TO_METHODS = {
    # long processes
    'start-campaign': WORKER.start_campaign,
    'resume-campaign': WORKER.resume_campaign,
    'process-contact': WORKER.process_contact,
    'process-event': WORKER.process_event,
    'process-campaign': WORKER.process_campaign,
    'stop-dialer': WORKER.stop_dialer,
    # short processes
    'create-campaign': WORKER.create_campaign,
    'edit-campaign': WORKER.edit_campaign,
    'stop-campaign': WORKER.stop_campaign,
    'pause-campaign': WORKER.pause_campaign,
    'delete-campaign': WORKER.delete_campaign,
    'add-incidence-rule-disposition': WORKER.add_incidence_rule_disposition,
    'create-incidence-rule': WORKER.create_incidence_rule,
    'delete-incidence-rule': WORKER.delete_incidence_rule,
    'update-incidence-rule': WORKER.update_incidence_rule,
    'change-database': WORKER.change_database,
    'render-template': WORKER.render_template,
    # medium processes
    'send-reports': WORKER.send_reports,
    # scheduled processes
    'schedule-contact': WORKER.schedule_contact,
    'schedule-agenda': WORKER.schedule_agenda
}


for job_name in GEARMAN_JOBS:
    job_name_bytes = job_name.encode(encoding='utf8')
    gm_worker.register_task(
        job_name_bytes,
        JOBS_TO_METHODS[job_name]
    )

gm_worker.work()
