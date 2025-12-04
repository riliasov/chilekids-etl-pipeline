-- Recommended SQL Fixes based on Data Audit

-- 1. Drop columns that are 100% NULL (if not needed for future use)
-- These columns were found to be empty in the sample:
-- sheet_row_number, cat_new, cat_final, subcat_new, payment_date_orig, subcat_final

-- ALTER TABLE staging.records DROP COLUMN IF EXISTS cat_new;
-- ALTER TABLE staging.records DROP COLUMN IF EXISTS cat_final;
-- ALTER TABLE staging.records DROP COLUMN IF EXISTS subcat_new;
-- ALTER TABLE staging.records DROP COLUMN IF EXISTS subcat_final;
-- ALTER TABLE staging.records DROP COLUMN IF EXISTS payment_date_orig;

-- 2. Investigate high NULL percentage columns
-- hours (85% NULL), count_vendor (72% NULL), sum_total_rub (72% NULL)
-- Consider if these should be nullable or have a default value.

-- 3. Type Safety
-- Ensure numeric columns are strictly typed
-- ALTER TABLE staging.records ALTER COLUMN total_rub TYPE NUMERIC USING total_rub::NUMERIC;
