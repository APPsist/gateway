var GATEWAY = (function($) {
	var logEntryTemplate;
	
	var init = function() {
		var tplSrc = $('#tpl-log-entry').html();
		logEntryTemplate = Handlebars.compile(tplSrc);
		
		update();
		
		$('#highlightInput').on('input', function(event) {
			highlightEntries();
		});
		
	};
	
	function highlightEntries() {
		var textToHighlight = $('#highlightInput').val();
		$.each($('.log-entry'), function(index, e) {
			var entry = $(e);
			if (textToHighlight) {
				if (entry.text().toLowerCase().indexOf(textToHighlight.toLowerCase()) >= 0) {
					entry.addClass('highlighted');
					entry.removeClass('passive');
				} else {
					entry.removeClass('highlighted');
					entry.addClass('passive');
				}
			} else {
				entry.removeClass('highlighted');
				entry.removeClass('passive');
			}
		});
	}
	
	function update() {
		var num = $('#numInput').val();
		var desc = $('#descInput').prop('checked');
		var level = $('#levelInput').val();
		var logsDiv = $('#logs');
		logsDiv.empty();
		
		$.get('/log?num=' + num + '&level=' + level, {}, function(data) {
			if (data.status == "ok") {
				console.log("Retrieved log data. Entries: " + data.entries.length);
				$.each(desc ? data.entries : data.entries.reverse(), function(index, entry) {
					if (index % 2 == 0) {
						entry.odd = true;
					}
					var entryHTML = logEntryTemplate(entry);
					var entryObject = $(entryHTML);
					entryObject.on('click', {json : entry}, function(event) {
						alert(JSON.stringify(event.data.json, null, 2));
					});
					entryObject.addClass(entry.level.toLowerCase());
					logsDiv.append(entryObject);
				});
				highlightEntries();
			} else {
				console.log("Failed to retrieve log data: " + data.message);
			}
		}, 'json');
	}
	
	return {
		init : init,
		update : update
	}
})($);