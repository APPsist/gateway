var GATEWAY = (function($) {
	var serviceEntryTemplate;
	
	var init = function() {
		var tplSrc = $('#tpl-service-row').html();
		serviceEntryTemplate = Handlebars.compile(tplSrc);
		
		update();
	};
	
	function update() {
		var tbody = $('table#services > tbody');
		tbody.empty();
		
		$.get('/status', {}, function(data) {
			if (data.status == "ok") {
				var systemStatus = data.systemStatus;
				console.log("Retrieved system status data.", systemStatus);
				$('#lastUpdate').text(systemStatus.created);
				
				var serviceIds = Object.keys(systemStatus.services);
				serviceIds.sort();
				$.each(serviceIds, function (index, serviceId) {
					var service = systemStatus.services[serviceId];
					service.id = serviceId;
					var rowHTML = serviceEntryTemplate(service);
					var row = $(rowHTML);
					tbody.append(row);
					var statusDisplay = row.find('.statusDisplay');
					statusDisplay.addClass(service.status.toLowerCase());
					statusDisplay.on('click', {service : service}, function(event) {
						alert(JSON.stringify(event.data.service.lastStatusSignal, null, 4));
					});
					row.find('.showConfigBtn').on('click', {service : service}, function(event) {
						alert(JSON.stringify(event.data.service.serviceConfiguration, null, 4));
					});
				});
			} else {
				console.log("Failed to retrieve system status data: " + data.message);
			}
		}, 'json');
	}
	
	return {
		init : init,
		update : update
	}
})($);