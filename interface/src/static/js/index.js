// htmx.logAll();

var $datatable;

document.body.addEventListener('htmx:load', function(evt) {
    $datatable = $('#example').DataTable();

    $datatable.on('draw.dt', function () {
        htmx.process('#example');
    });
});



htmx.defineExtension('ws-transform-data', {
    transformResponse: function(response, xhr, elt) {
        if (!xhr && ( elt.hasAttribute('ws-connect') || elt.hasAttribute('ws-send') )) {
            var responseJson = JSON.parse(response);
            switch (responseJson.args.type) {
            case 'DELETE':
                $datatable
                    .row($('#actions-'+responseJson['camp_id']).parents('tr'))
                    .remove()
                    .draw(false);
                break;
            case 'CREATE':
                // TODO: use $datatable.row.add(...).draw(false).node() and htmx.process()
                // in order to avoid this full page refresh
                // the content rendered can be sent by the backend as part of the websocket message
                location.reload(true);
                break;
            case 'STATS':
                var $modalNode = $(".modal-body");
                var idCampaign = $("#campaignInfo").val();
                if (idCampaign == responseJson.args['camp_id']) {
                    $modalNode.html(responseJson.args.admin);
                }
                break;
            case 'PAUSE_BULK':
                // for pause of all active campaigns
                // TODO: use Datatables API or see if with HTMX it is possible to something
                location.reload(true);
                break;
            default:
                if (responseJson.args.admin !== undefined) {
                    response = responseJson.args.admin;
                }
            }
        }
        return response;
    }
})
