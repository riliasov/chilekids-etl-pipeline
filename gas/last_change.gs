function onEdit(e) {
  // Получаем диапазон изменений и лист
  var range = e.range;
  var sheet = range.getSheet();
  
  // Проверяем, что это нужный лист (замените 'Sheet1' на имя вашего листа, если нужно)
  if (sheet.getName() !== 'Продажи') return;
  
  // Получаем начальную строку и количество строк в изменённом диапазоне
  var startRow = range.getRow();
  var numRows = range.getNumRows();
  
  // Получаем начальную и конечную колонку изменения
  var startCol = range.getColumn();
  var endCol = range.getLastColumn();
  
  // Если изменение не пересекает колонки A:T (1-20), выходим
  if (endCol < 1 || startCol > 20) return;
  
  // Получаем время в UTC
  var timestamp = new Date();
  
  // Форматируем в местное время Asia/Yekaterinburg
  var timeZone = "Asia/Yekaterinburg";
  var formattedTimestamp = Utilities.formatDate(timestamp, timeZone, "dd.MM.yyyy HH:mm:ss");
  
  // Получаем email
  var email = e.user.getEmail();
  
  // Обходим все затронутые строки
  for (var i = 0; i < numRows; i++) {
    var currentRow = startRow + i;
    
    // Пропускаем строки выше 3 (заголовки в строке 2, данные с 3)
    if (currentRow < 3) continue;
    
    // Записываем в U (колонка 21) - last_change (форматированное местное время как строка)
    sheet.getRange(currentRow, 21).setValue(formattedTimestamp);
    
    // Записываем в V (колонка 22) - changed_by (email)
    sheet.getRange(currentRow, 22).setValue(email);
  }
}