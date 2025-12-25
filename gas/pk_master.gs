/**
 * PK MASTER: FINAL CORE EDITION
 * Назначение: Быстрая и безопасная проливка PK для исторических данных.
 */

/* ====== КОНФИГУРАЦИЯ ====== */
const CFG = {
  headerRow: 2,             // Строка с заголовками (по умолчанию 2, может быть переопределено)
  pkHeaderName: 'PK',      // Как называется колонка PK
  prefix: 'sa',             // Префикс ключа
  pad: 6,                   // Длина цифр (000000)
  startRow: 3,              // Первая строка с данными
  requiredCols: [1, 2, 3],  // PK ставится только если эти колонки (A, B, C) НЕ пусты
  force: false              // false = не трогать уже существующие ID
};

/**
 * Создание меню при открытии таблицы
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('🚀 PK Master')
      .addItem('Присвоить PK (текущий лист)', 'runPKAssignment')
      .addToUi();
}

/* ====== ЦЕНТРАЛЬНЫЙ ДВИЖОК (Core Logic) ====== */
const PK_ENGINE = {
  // Генерация строки ID
  generate: (prefix, pad, num) => `${prefix}_${String(num).padStart(pad, '0')}`,

  // Проверка, валидна ли строка для присвоения PK
  isValidRow: (rowData, indices) => {
    if (!indices || indices.length === 0) return rowData.some(c => String(c).trim() !== '');
    return indices.every(idx => rowData[idx - 1] && String(rowData[idx - 1]).trim() !== '');
  },

  // Поиск индекса колонки по имени заголовка
  findColumnByName: (sheet, headerRow, name) => {
    const lastCol = sheet.getLastColumn();
    if (lastCol === 0) return -1;
    const headers = sheet.getRange(headerRow, 1, 1, lastCol).getValues()[0];
    for (let i = 0; i < headers.length; i++) {
      if (String(headers[i]).trim().toUpperCase() === name.toUpperCase()) return i + 1;
    }
    return -1;
  }
};

/**
 * ГЛАВНАЯ ФУНКЦИЯ ЗАПУСКА
 */
function runPKAssignment() {
  const ui = SpreadsheetApp.getUi();
  const lock = LockService.getScriptLock();
  
  try {
    // 1. Пытаемся захватить монопольный доступ на 30 сек
    if (!lock.tryLock(30000)) {
      ui.alert('Ошибка: Скрипт уже запущен в другом процессе.');
      return;
    }

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getActiveSheet(); // Работаем с текущим активным листом
    
    // 2. Определяем координаты колонки PK
    const pkCol = PK_ENGINE.findColumnByName(sheet, CFG.headerRow, CFG.pkHeaderName);
    if (pkCol === -1) {
      throw new Error(`Колонка с заголовком "${CFG.pkHeaderName}" не найдена в строке ${CFG.headerRow}.`);
    }

    // 3. Загружаем данные
    const lastRow = sheet.getLastRow();
    if (lastRow < CFG.startRow) {
      ui.alert('Предупреждение: Данные отсутствуют или startRow указана неверно.');
      return;
    }

    const numRows = lastRow - CFG.startRow + 1;
    const allData = sheet.getRange(CFG.startRow, 1, numRows, sheet.getLastColumn()).getValues();
    const currentPKs = sheet.getRange(CFG.startRow, pkCol, numRows, 1).getValues();

    // 4. Определяем стартовый номер для инкремента
    let nextIdNum = 1;
    const idRegex = new RegExp(`^${CFG.prefix}_(\\d+)$`, 'i');
    currentPKs.flat().forEach(val => {
      const match = String(val).match(idRegex);
      if (match) nextIdNum = Math.max(nextIdNum, parseInt(match[1], 10) + 1);
    });

    // 5. Обработка массива
    let createdCount = 0;
    let skippedEmpty = 0;
    
    const finalPKs = currentPKs.map((row, i) => {
      const existingValue = String(row[0]).trim();
      
      // Если PK есть и мы не перезаписываем — оставляем
      if (existingValue !== '' && !CFG.force) return [existingValue];

      // Проверка на "пустоту" по правилам CFG
      if (!PK_ENGINE.isValidRow(allData[i], CFG.requiredCols)) {
        skippedEmpty++;
        return [''];
      }

      // Создаем новый ID
      createdCount++;
      return [PK_ENGINE.generate(CFG.prefix, CFG.pad, nextIdNum++)];
    });

    // 6. Запись в таблицу одной операцией
    if (createdCount > 0 || CFG.force) {
      sheet.getRange(CFG.startRow, pkCol, numRows, 1).setValues(finalPKs);
      ui.alert(`Успешно!\nСоздано PK: ${createdCount}.\nПропущено пустых строк: ${skippedEmpty}.`);
    } else {
      ui.alert('Изменений не требуется (все строки уже имеют PK).');
    }

  } catch (e) {
    ui.alert(`Критическая ошибка: ${e.message}`);
    console.error(e.stack);
  } finally {
    // 7. Всегда освобождаем доступ
    lock.releaseLock();
  }
}
