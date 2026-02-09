-- Script to update test_users table with updated_at column
-- Required for column_pair_comparison check tests

-- Add updated_at column if it doesn't exist
ALTER TABLE test_users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

-- Set updated_at to be slightly after created_at for existing rows
UPDATE test_users
SET updated_at = created_at + INTERVAL '1 hour'
WHERE updated_at IS NULL;
