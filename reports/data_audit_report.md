# Data Audit Report

**Date:** 2025-11-21 10:08:00
**Rows Sampled:** 100

## Field Analysis

| Field | Schema Type | Observed Types | % Null | Unique | Errors | Examples |
|-------|-------------|----------------|--------|--------|--------|----------|
| raw_id | BIGINT | int | 0.0% | 100 | 0 | 1, 2, 3 |
| sheet_row_number | INTEGER |  | 100.0% | 0 | 0 |  |
| received_at | TIMESTAMPTZ | Timestamp | 0.0% | 2 | 0 | 2025-11-19 22:28:06.584963+00:00, 2025-11-19 22:28:06.584963+00:00, 2025-11-19 22:28:06.584963+00:00 |
| date | TIMESTAMPTZ | Timestamp | 0.0% | 43 | 0 | 2023-07-16 00:00:00+00:00, 2023-07-16 00:00:00+00:00, 2023-08-31 00:00:00+00:00 |
| payment_date | TIMESTAMPTZ | Timestamp | 22.0% | 27 | 0 | 2024-07-16 00:00:00+00:00, 2024-07-16 00:00:00+00:00, 2023-08-31 00:00:00+00:00 |
| task | TEXT | str | 0.0% | 15 | 0 | Доход франчайзи, Комиссия франчайзи, Комиссия франчайзи |
| type | TEXT | str | 0.0% | 3 | 0 | Доход, Доход, Расход |
| year | INTEGER | int | 0.0% | 1 | 0 | 2023, 2023, 2023 |
| hours | NUMERIC | Decimal | 85.0% | 2 | 0 | 1, 2, 2 |
| month | INTEGER | int | 0.0% | 6 | 0 | 7, 7, 8 |
| client | TEXT | str | 0.0% | 44 | 0 | Ступак Ангелина чк, Ступак Ангелина чк, Самохина Ирина чп |
| fx_rub | NUMERIC | Decimal | 2.0% | 25 | 0 | 90.25, 90.25, 95.625 |
| fx_usd | NUMERIC | Decimal | 2.0% | 29 | 0 | 1, 1, 1 |
| vendor | TEXT | str | 0.0% | 15 | 0 | FRANCHISE Москва (доход), FRANCHISE Москва (расход), FRANCHISE Москва (расход) |
| cashier | TEXT | str | 0.0% | 3 | 0 | Франшиза Москва, Франшиза Москва, Франшиза Москва |
| cat_new | TEXT |  | 100.0% | 0 | 0 |  |
| quarter | INTEGER | int | 0.0% | 2 | 0 | 3, 3, 3 |
| service | TEXT | str | 0.0% | 4 | 0 | Kids, Kids, Passport |
| approver | TEXT | str | 0.0% | 1 | 0 | , ,  |
| category | TEXT | str | 0.0% | 7 | 0 | Доходы, Доходы, Маркетинг |
| currency | TEXT | str | 0.0% | 4 | 0 | USD, USD, USD |
| cat_final | TEXT |  | 100.0% | 0 | 0 |  |
| total_rub | NUMERIC | Decimal | 2.0% | 58 | 0 | 128606, 128606, 95625 |
| total_usd | NUMERIC | Decimal | 2.0% | 46 | 0 | 1425, 1425, 1000 |
| subcat_new | TEXT |  | 100.0% | 0 | 0 |  |
| paket | TEXT | str | 0.0% | 9 | 0 | Комфорт, Комфорт, По родственнику |
| description | TEXT | str | 0.0% | 76 | 0 | 1 платеж 1425$ (напрямую франчайзи) по договору на роды за 5700$, 1 платеж 1425$ (напрямую франчайзи) по договору на роды за 5700$, 1 платеж 1000$ по договору на гражданство за 6250$ |
| subcategory | TEXT | str | 0.0% | 21 | 0 | Прямые доходы, Комиссия, Онлайн реклама |
| payment_date_orig | TIMESTAMPTZ |  | 100.0% | 0 | 0 |  |
| subcat_final | TEXT |  | 100.0% | 0 | 0 |  |
| count_vendor | INTEGER | float | 72.0% | 27 | 0 | 1353.0, 159.0, 292.0 |
| statya | TEXT | str | 0.0% | 6 | 0 | Доходы, Агентские расходы, Агентские расходы |
| sum_total_rub | NUMERIC | Decimal | 72.0% | 28 | 0 | 195103884.5, 8407068.09, 10333716.88 |
| usd_summa | NUMERIC | Decimal | 2.0% | 59 | 0 | -1425, 1425, 1000 |
| direct_indirect | TEXT | str | 0.0% | 2 | 0 | Direct, Direct, Direct |
| package_secondary | TEXT | str | 0.0% | 8 | 0 | Комфорт, ,  |
| total_in_currency | NUMERIC | Decimal | 1.0% | 69 | 0 | -1425, 1425, 1000 |
| rub_summa | NUMERIC | Decimal | 2.0% | 76 | 0 | -128606, 128606, 95625 |
| kategoriya | TEXT | str | 0.0% | 5 | 0 | Доходы, Продажи, Продажи |
| podstatya | TEXT | str | 0.0% | 10 | 0 | Доход франчайзи, Комиссия франчайзи, Комиссия франчайзи |
| vidy_raskhodov | TEXT | str | 0.0% | 3 | 0 | Доходы, Переменные, Переменные |
| payload_hash | TEXT | str | 0.0% | 100 | 0 | f4abc9634528cfb5bdc15173991a05c3, 3250108a062fc83104ee2f6f3cfc5551, f0aad250de6a9f5635ea7d8e84740fe2 |
| raw_payload | JSONB | str | 0.0% | 100 | 0 | {"": "", "Date": "16.07.2023", "Task": "Доход франчайзи", "Type": "Доход", "Year": "2023", "Hours": "", "Month": "7", "Client": "Ступак Ангелина чк", "FX RUB": "90,25", "FX USD": "1", "Vendor": "FRANCHISE Москва (доход)", "Cashier": "Франшиза Москва", "Cat new": "Доходы", "Quarter": "3", "Service": "Kids", "Approver": "", "Category": "Доходы", "Currency": "USD", "Cat FInal": "Доходы", "Total RUB": "128606", "Total USD": "1425", "SubCat new": "Франшиза", "Пакет": "Комфорт", "Description": "1 платеж 1425$ (напрямую франчайзи) по договору на роды за 5700$", "SubCategory": "Прямые доходы", "Payment date": "16.07.2024", "SubCat Final": "", "count Vendor": "1353", "Статья": "Доходы", "sum Total RUB": "195103884,5", "USD сумма": "-1425", "Direct / Indirect": "Direct", "Package secondary": "Комфорт", "Total in currency": "-1425", "РУБ сумма": "-128606", "Категория": "Доходы", "Подстатья": "Доход франчайзи", "Виды расходов": "Доходы"}, {"": "", "Date": "16.07.2023", "Task": "Комиссия франчайзи", "Type": "Доход", "Year": "2023", "Hours": "", "Month": "7", "Client": "Ступак Ангелина чк", "FX RUB": "90,25", "FX USD": "1", "Vendor": "FRANCHISE Москва (расход)", "Cashier": "Франшиза Москва", "Cat new": "Продажи", "Quarter": "3", "Service": "Kids", "Approver": "", "Category": "Доходы", "Currency": "USD", "Cat FInal": "Продажи", "Total RUB": "128606", "Total USD": "1425", "SubCat new": "", "Пакет": "Комфорт", "Description": "1 платеж 1425$ (напрямую франчайзи) по договору на роды за 5700$", "SubCategory": "Комиссия", "Payment date": "16.07.2024", "SubCat Final": "Комиссия", "count Vendor": "159", "Статья": "Агентские расходы", "sum Total RUB": "8407068,09", "USD сумма": "1425", "Direct / Indirect": "Direct", "Package secondary": "", "Total in currency": "1425", "РУБ сумма": "128606", "Категория": "Продажи", "Подстатья": "Комиссия франчайзи", "Виды расходов": "Переменные"}, {"": "", "Date": "31.08.2023", "Task": "Комиссия франчайзи", "Type": "Расход", "Year": "2023", "Hours": "", "Month": "8", "Client": "Самохина Ирина чп", "FX RUB": "95,625", "FX USD": "1", "Vendor": "FRANCHISE Москва (расход)", "Cashier": "Франшиза Москва", "Cat new": "Продажи", "Quarter": "3", "Service": "Passport", "Approver": "", "Category": "Маркетинг", "Currency": "USD", "Cat FInal": "Продажи", "Total RUB": "95625", "Total USD": "1000", "SubCat new": "", "Пакет": "По родственнику", "Description": "1 платеж 1000$ по договору на гражданство за 6250$", "SubCategory": "Онлайн реклама", "Payment date": "31.08.2023", "SubCat Final": "Комиссия", "count Vendor": "292", "Статья": "Агентские расходы", "sum Total RUB": "10333716,88", "USD сумма": "1000", "Direct / Indirect": "Direct", "Package secondary": "", "Total in currency": "1000", "РУБ сумма": "95625", "Категория": "Продажи", "Подстатья": "Комиссия франчайзи", "Виды расходов": "Переменные"} |

## Recommendations

- [ ] Review fields with high NULL percentage.
- [ ] Fix type mismatches where observed types differ from schema.
