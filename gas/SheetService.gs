/**
 * SHEET SERVICE: IDENTITY & CDC EDITION
 * Назначение: Управление UUID, отслеживание изменений и метаданных.
 * 
 * ФУНКЦИИ:
 * 1. onOpen: Меню для ручного управления.
 * 2. onEdit: Автоматический трекинг изменений (updated_at/by).
 * 3. assignIdentity: Массовое присвоение UUID и created_at.
 */

/* ====== КОНФИГУРАЦИЯ ====== */
const CFG = {
  headerRow: 2,             // Строка с заголовками
  pkHeader: 'PK',           // Название колонки Primary Key (UUID)
  createdHeader: 'created_at',
  updatedHeader: 'updated_at',
  updatedByHeader: 'updated_by',
  hashHeader: 'content_hash', // Опционально: хеш контента
  
  startRow: 3,              // Первая строка с данными
  requiredCols: [1, 2, 3],  // Для валидации заполненности (A, B, C)
  
  timeZone: "Asia/Yekaterinburg",
  dateTimeFormat: "dd.MM.yyyy HH:mm:ss"
};

/**
 * Создание меню
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('🚀 Sheet Service')
    .addItem('Присвоить Identity (массово)', 'runIdentityAssignment')
    .addToUi();
}

/**
 * Автоматический трекинг изменений
 */
function onEdit(e) {
  const range = e.range;
  const sheet = range.getSheet();
  const startRow = range.getRow();
  const numRows = range.getNumRows();
  
  // Пропускаем заголовки
  if (startRow < CFG.startRow) return;

  const lock = LockService.getScriptLock();
  try {
    if (!lock.tryLock(5000)) return;

    const lastCol = sheet.getLastColumn();
    const headers = sheet.getRange(CFG.headerRow, 1, 1, lastCol).getValues()[0];
    
    // Ищем индексы мета-колонок
    const idx = {
      pk: headers.indexOf(CFG.pkHeader) + 1,
      created: headers.indexOf(CFG.createdHeader) + 1,
      updated: headers.indexOf(CFG.updatedHeader) + 1,
      updatedBy: headers.indexOf(CFG.updatedByHeader) + 1
    };

    if (idx.updated === 0) return; // Нет колонки трекинга - выходим

    const now = Utilities.formatDate(new Date(), CFG.timeZone, CFG.dateTimeFormat);
    const email = Session.getActiveUser().getEmail() || "anonymous";

    for (let i = 0; i < numRows; i++) {
      let currentRow = startRow + i;
      
      // 1. Обновляем время изменения
      sheet.getRange(currentRow, idx.updated).setValue(now);
      if (idx.updatedBy > 0) sheet.getRange(currentRow, idx.updatedBy).setValue(email);

      // 2. Если UUID нет - создаем его (автоматически для новых строк)
      if (idx.pk > 0) {
        let pkCell = sheet.getRange(currentRow, idx.pk);
        if (!pkCell.getValue()) {
          pkCell.setValue(generateUUID());
          if (idx.created > 0) sheet.getRange(currentRow, idx.created).setValue(now);
        }
      }
    }
  } finally {
    lock.releaseLock();
  }
}

/**
 * Ручной запуск проливки ID для старых данных
 */
function runIdentityAssignment() {
  const ui = SpreadsheetApp.getUi();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getActiveSheet();
  const lock = LockService.getScriptLock();
  
  try {
    if (!lock.tryLock(30000)) {
      ui.alert('Ошибка: Доступ заблокирован другим процессом.');
      return;
    }

    const lastRow = sheet.getLastRow();
    const lastCol = sheet.getLastColumn();
    if (lastRow < CFG.startRow) {
      ui.alert('Данные не найдены.');
      return;
    }

    const headers = sheet.getRange(CFG.headerRow, 1, 1, lastCol).getValues()[0];
    const pkIdx = headers.indexOf(CFG.pkHeader) + 1;
    const createdIdx = headers.indexOf(CFG.createdHeader) + 1;

    if (pkIdx === 0) throw new Error(`Колонка "${CFG.pkHeader}" не найдена.`);

    const numRows = lastRow - CFG.startRow + 1;
    const pkRange = sheet.getRange(CFG.startRow, pkIdx, numRows, 1);
    const pkValues = pkRange.getValues();
    
    const now = Utilities.formatDate(new Date(), CFG.timeZone, CFG.dateTimeFormat);
    let createdCount = 0;

    const newValues = pkValues.map((row, i) => {
      if (!row[0]) {
        createdCount++;
        // Если есть колонка created_at - проставляем и её
        if (createdIdx > 0) {
          sheet.getRange(CFG.startRow + i, createdIdx).setValue(now);
        }
        return [generateUUID()];
      }
      return [row[0]];
    });

    if (createdCount > 0) {
      pkRange.setValues(newValues);
      ui.alert(`Успешно! Присвоено новых ID: ${createdCount}`);
    } else {
      ui.alert('Все записи уже имеют ID.');
    }

  } catch (e) {
    ui.alert('Ошибка: ' + e.message);
  } finally {
    lock.releaseLock();
  }
}

/**
 * Вспомогательная функция генерации UUID
 */
function generateUUID() {
  return Utilities.getUuid();
}
