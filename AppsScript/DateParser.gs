// Mapping month names to numbers
var monthMap = {
  "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, 
  "June": 6, "July": 7, "August": 8, "September": 9, "October": 10, 
  "November": 11, "December": 12
};

// Function to parse event start date and return in "YYYY-MM-DD HH:MM" format
function parseEventDate(eventStr) {
  // Split the date string (e.g., "March 1st 4pm")
  var parts = eventStr.split(" ");
  var month = monthMap[parts[0]];  // Extract month (e.g., "March" -> 3)
  var day = parseInt(parts[1].replace(/\D/g, ""));  // Remove "st", "nd", "rd", "th" from day
  var timeStart = parts[2].trim();  // e.g., "4pm"

  // Convert start time to 24-hour format
  var hour = parseInt(timeStart);
  var period = timeStart.slice(-2);  // "am" or "pm"
  if (period === "pm" && hour !== 12) hour += 12;
  if (period === "am" && hour === 12) hour = 0;

  // Get the current year
  var year = new Date().getFullYear();

  // Construct the Date object and format it
  var date = new Date(year, month - 1, day, hour, 0, 0);
  var formattedDate = `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')} ${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}`;

  return formattedDate;  // Return the formatted date
}
