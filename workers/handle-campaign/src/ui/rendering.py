# -*- coding: utf-8 -*-

from jinja2 import Environment, PackageLoader, select_autoescape

STATUS_TO_ACTION = {2: "Activate", 3: "Finalize", 5: "Pause"}
STATUS_TO_ACTION_URLS = {2: "start", 3: "stop", 5: "pause"}

ENVIRONMENT = Environment(
    loader=PackageLoader("ui"),
    autoescape=select_autoescape()
)

INIT_TEMPLATE = ENVIRONMENT.get_template("htmx/init.html")
ROW_TEMPLATE = ENVIRONMENT.get_template("htmx/campaign_row.html")
STATS_TEMPLATE = ENVIRONMENT.get_template("htmx/statistics.html")
STATS_INNER_TEMPLATE = ENVIRONMENT.get_template("htmx/statistics_inner.html")

ENVIRONMENT.globals['STATUS_TO_ACTION_URLS'] = STATUS_TO_ACTION_URLS
ENVIRONMENT.globals['STATUS_TO_ACTION'] = STATUS_TO_ACTION


class AdminRender():
    @classmethod
    def render_init(cls, campaigns):
        return INIT_TEMPLATE.render(campaigns=campaigns, running=True)

    @classmethod
    def render_status_change(cls, id_campaign, new_status, new_status_str, actions_campaign):
        return ROW_TEMPLATE.render(
            id_campaign=id_campaign,
            status=new_status,
            status_campaign=new_status_str,
            actions_campaign=actions_campaign)

    @classmethod
    def render_stats(cls, id_campaign, stats):
        return STATS_TEMPLATE.render(
            id_campaign=id_campaign,
            statistics=stats
        )

    @classmethod
    def render_stats_inner(cls, id_campaign, stats):
        stats_for_render = stats.copy()
        return STATS_INNER_TEMPLATE.render(
            id_campaign=id_campaign,
            statistics=stats_for_render
        )
