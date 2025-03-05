function generateJsonFromResponses() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('March Responses');
  var peepsSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('All Novice Peeps Members');
  
  // Get all data from the "All Novice Peeps Members" sheet
  var peepsData = peepsSheet.getDataRange().getValues();
  
  // Set up the headers and data structure
  var jsonData = [];
  var uniqueEvents = new Map(); // Store unique events with IDs
  var uniquePeeps = new Map(); // Store unique people with IDs
  var eventCounter = 0;
  
  // Loop through the "All Novice Peeps Members" sheet to populate peeps
  for (var i = 1; i < peepsData.length; i++) {
    var row = peepsData[i];
    var id = row[0];
    var name = row[1].trim();  // Remove any extra whitespace
    var role = row[3];
    var index = row[4];
    var priority = row[5];
    var totalAttended = row[6];

    // Ensure the person (peep) is unique
    if (!uniquePeeps.has(id)) {
      uniquePeeps.set(id, {
        "id": id,
        "name": name,
        "role": role,
        "index": index,
        "priority": priority,
        "total_attended": totalAttended,
        "availability": [],  // Will be populated later
        "event_limit": 0     // Will be populated later
      });
    }
  }

  // Now, retrieve responses from "March Responses" sheet
  var data = sheet.getDataRange().getValues();
  
  // Process each row in the "March Responses" sheet (skip header)
  for (var i = 1; i < data.length; i++) {
    var row = data[i];
    var name = row[1].trim();  // Remove extra whitespace from name in response
    var preferredRole = row[2];
    var maxSessions = row[3];

    // Attempt to find a matching peep based on full name first
    var matchedPeeps = [];
    uniquePeeps.forEach(function(peep, id) {
      var peepName = peep.name.trim().toLowerCase();
      if (peepName === name.toLowerCase()) {
        matchedPeeps.push(peep);
      }
    });

    // If no match based on full name, attempt matching by first name only
    if (matchedPeeps.length === 0) {
      uniquePeeps.forEach(function(peep, id) {
        var peepFirstName = peep.name.trim().toLowerCase().split(' ')[0];  // Extract first name only
        if (peepFirstName === name.toLowerCase()) {
          matchedPeeps.push(peep);
        }
      });
    }

    // Handle matching cases
    if (matchedPeeps.length === 1) {
      var peep = matchedPeeps[0];

      // Set the event_limit based on the response
      peep.event_limit = maxSessions;

      // Process available dates from the response
      var availableDates = row[4].split(/,\s*/);  // Split comma-separated dates

      // Map the available dates to event IDs
      var eventIds = availableDates.map(event => {
        if (!uniqueEvents.has(event)) {
          var formattedDate = parseEventDate(event); // Call the function from DateParser.gs
          uniqueEvents.set(event, {
            "id": eventCounter++,
            "date": formattedDate,  // Store date in the correct format
            "min_role": 4, // Default values for min/max role
            "max_role": 6
          });
        }
        return uniqueEvents.get(event).id;
      });

      // Add event IDs to the peep's availability list (avoid duplicates)
      peep.availability = [...new Set([...peep.availability, ...eventIds])];

      // Create the response for this peep
      var response = {
        "timestamp": row[0],  // Timestamp of the form submission
        "name": name,
        "preferred_role": preferredRole,
        "max_sessions": maxSessions,
        "available_dates": availableDates, // Keep the original date format from the form
        "comments": row[5] || ""  // Handle empty comments
      };
      jsonData.push(response);
    } else if (matchedPeeps.length === 0) {
      // Log error if no match is found for the name
      Logger.log("Error: Peep with name '" + name + "' not found in the members list. Please fix the response.");
    } else {
      // Log error if more than one match is found for the name
      Logger.log("Error: Multiple matches found for name '" + name + "'. Please resolve the ambiguity.");
    }
  }

  // Check if data is populated
  if (jsonData.length === 0 || uniqueEvents.size === 0 || uniquePeeps.size === 0) {
    Logger.log("Error: Data is empty, check if the responses and peep sheets are properly populated.");
    return;
  }

  // Generate the final JSON output with responses at the top
  var output = {
    "responses": jsonData,  // Responses come first for readability
    "events": Array.from(uniqueEvents.values()),  // Convert events Map to array
    "peeps": Array.from(uniquePeeps.values())    // Convert peeps Map to array
  };

  var jsonOutput = JSON.stringify(output, null, 2);  // Format JSON output with 2 spaces for readability

  // Get the name of the sheet to use in the file name
  var sheetName = sheet.getName().toLowerCase().replace(/\s+/g, '_');  // Format the sheet name to use as part of the filename

  // Create the output file with dynamic name based on the sheet name
  var fileName = 'novice_peeps_' + sheetName + '_output.json';
  var folder = DriveApp.getRootFolder();  // You can specify another folder if needed

  // Check if a file already exists with the same name
  var files = folder.getFilesByName(fileName);
  while (files.hasNext()) {
    var file = files.next();
    file.setTrashed(true);  // Trash the existing file
  }

  // Now create the new file with the same name
  var file = folder.createFile(fileName, jsonOutput, MimeType.PLAIN_TEXT);
  
  Logger.log("JSON output saved to file: " + file.getName());
  return jsonOutput;
}
