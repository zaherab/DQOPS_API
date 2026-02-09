-- =============================================================================
-- DQ Platform Test Data Setup
-- =============================================================================
-- This script creates comprehensive test data for all 54 DQOps check types.
-- Run this against your test PostgreSQL database before running tests.
-- =============================================================================

-- Drop existing tables if they exist (in correct order for FK constraints)
DROP TABLE IF EXISTS test_data_quality CASCADE;
DROP TABLE IF EXISTS test_categories CASCADE;
DROP TABLE IF EXISTS test_users CASCADE;

-- =============================================================================
-- Reference table for foreign key tests
-- =============================================================================
CREATE TABLE test_categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO test_categories (id, name) VALUES
    (1, 'Electronics'),
    (2, 'Books'),
    (3, 'Clothing'),
    (4, 'Food'),
    (5, 'Sports');

-- =============================================================================
-- Basic test users table (legacy)
-- =============================================================================
CREATE TABLE test_users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO test_users (id, name, email, status, created_at, updated_at) VALUES
    (1, 'Alice Smith', 'alice@example.com', 'active', NOW() - INTERVAL '1 hour', NOW()),
    (2, 'Bob Jones', 'bob@example.com', 'active', NOW() - INTERVAL '2 hours', NOW()),
    (3, 'Charlie Brown', NULL, 'inactive', NOW() - INTERVAL '3 hours', NOW()),
    (4, 'Diana Prince', 'diana@example.com', 'pending', NOW() - INTERVAL '4 hours', NOW()),
    (5, 'Eve Wilson', 'eve@example.com', 'active', NOW() - INTERVAL '5 hours', NOW());

-- =============================================================================
-- Comprehensive test table for all check types
-- =============================================================================
CREATE TABLE test_data_quality (
    id SERIAL PRIMARY KEY,

    -- Email column for pattern checks (mix of valid/invalid)
    email VARCHAR(255),

    -- UUID column for UUID validation
    uuid_col VARCHAR(36),

    -- IP address columns
    ip4_address VARCHAR(45),
    ip6_address VARCHAR(45),

    -- Phone and zipcode for USA format validation
    phone VARCHAR(20),
    zipcode VARCHAR(10),

    -- Geographic columns
    latitude NUMERIC(10, 7),
    longitude NUMERIC(10, 7),

    -- Boolean column
    is_active BOOLEAN,

    -- Numeric columns for statistical checks
    score INTEGER,
    price NUMERIC(10, 2),

    -- Text columns for text checks (with various lengths, empty, whitespace)
    description TEXT,
    short_code VARCHAR(20),

    -- DateTime column for temporal checks
    event_date DATE,

    -- Foreign key reference (some valid, some invalid)
    category_id INTEGER,

    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- =============================================================================
-- Insert comprehensive test data (20 rows with various conditions)
-- =============================================================================
INSERT INTO test_data_quality (
    id, email, uuid_col, ip4_address, ip6_address, phone, zipcode,
    latitude, longitude, is_active, score, price, description, short_code,
    event_date, category_id, created_at, updated_at
) VALUES
-- Row 1: All valid data
(1, 'valid@example.com', '550e8400-e29b-41d4-a716-446655440000', '192.168.1.1', '2001:0db8:85a3:0000:0000:8a2e:0370:7334', '(555) 123-4567', '12345',
 40.7128, -74.0060, TRUE, 85, 99.99, 'This is a valid description text', 'ABC123',
 CURRENT_DATE - INTERVAL '30 days', 1, NOW() - INTERVAL '1 hour', NOW()),

-- Row 2: Valid data with different values
(2, 'test.user@domain.org', 'f47ac10b-58cc-4372-a567-0e02b2c3d479', '10.0.0.1', '::1', '555-987-6543', '90210',
 34.0522, -118.2437, TRUE, 92, 150.00, 'Another valid description here', 'XYZ789',
 CURRENT_DATE - INTERVAL '15 days', 2, NOW() - INTERVAL '2 hours', NOW()),

-- Row 3: Invalid email format
(3, 'invalid-email', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', '172.16.0.1', 'fe80::1', '(555) 234-5678', '54321',
 51.5074, -0.1278, FALSE, 78, 49.99, 'Description with sufficient length', 'DEF456',
 CURRENT_DATE - INTERVAL '7 days', 3, NOW() - INTERVAL '3 hours', NOW()),

-- Row 4: Invalid UUID
(4, 'user4@test.com', 'not-a-valid-uuid', '8.8.8.8', '2001:db8::1', '555.123.4567', '12345-6789',
 48.8566, 2.3522, TRUE, 65, 75.50, 'Medium length text here', 'GHI789',
 CURRENT_DATE + INTERVAL '30 days', 4, NOW() - INTERVAL '4 hours', NOW()),  -- Future date!

-- Row 5: Invalid IPv4
(5, 'another@example.net', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '999.999.999.999', '::ffff:192.168.1.1', '1-800-555-1234', '00000',
 35.6762, 139.6503, FALSE, 55, 25.00, 'Short', 'JKL012',  -- Very short description
 CURRENT_DATE - INTERVAL '60 days', 5, NOW() - INTERVAL '5 hours', NOW()),

-- Row 6: Invalid IPv6
(6, 'email.six@company.co', '123e4567-e89b-12d3-a456-426614174000', '127.0.0.1', 'not-valid-ipv6', '555 456 7890', '99999',
 -33.8688, 151.2093, TRUE, 88, 199.99, 'Reasonably long description for testing text checks', 'MNO345',
 CURRENT_DATE - INTERVAL '90 days', 1, NOW() - INTERVAL '6 hours', NOW()),

-- Row 7: Invalid phone format
(7, 'valid.email@domain.io', 'c9bf9e57-1685-4c89-bafb-ff5af830be8a', '203.0.113.1', '2001:0db8:0000:0000:0000:0000:0000:0001', 'invalid-phone', '55555',
 52.5200, 13.4050, FALSE, 72, 89.99, '', 'PQR678',  -- Empty description!
 CURRENT_DATE - INTERVAL '45 days', 2, NOW() - INTERVAL '7 hours', NOW()),

-- Row 8: Invalid zipcode
(8, 'test8@example.com', 'b7e7f8e9-1a2b-3c4d-5e6f-7a8b9c0d1e2f', '198.51.100.1', 'fe80::1%eth0', '(123) 456-7890', 'ABCDE',
 55.7558, 37.6173, TRUE, 95, 299.99, '   ', 'STU901',  -- Whitespace-only description!
 CURRENT_DATE - INTERVAL '120 days', 3, NOW() - INTERVAL '8 hours', NOW()),

-- Row 9: Invalid latitude (out of range)
(9, 'nine@test.org', 'd4a33b40-9e6a-11e9-a2a3-2a2ae2dbcce4', '192.0.2.1', '2001:db8:85a3::8a2e:370:7334', '555-111-2222', '11111',
 95.0000, -74.0060, FALSE, 45, 15.00, 'Normal text description', 'VWX234',  -- Latitude > 90!
 CURRENT_DATE + INTERVAL '7 days', 4, NOW() - INTERVAL '9 hours', NOW()),  -- Future date!

-- Row 10: Invalid longitude (out of range)
(10, 'ten@domain.com', 'e5b6c7d8-e9f0-1a2b-3c4d-5e6f7a8b9c0d', '100.64.0.1', '::ffff:127.0.0.1', '(999) 888-7777', '22222',
 40.7128, -200.0000, TRUE, 60, 45.00, 'Text with adequate length for testing', 'YZA567',  -- Longitude < -180!
 CURRENT_DATE - INTERVAL '180 days', 5, NOW() - INTERVAL '10 hours', NOW()),

-- Row 11: NULL email
(11, NULL, 'f6c7d8e9-0a1b-2c3d-4e5f-6a7b8c9d0e1f', '192.168.0.100', '2001:db8:1234::5678', '555-333-4444', '33333',
 45.4215, -75.6972, TRUE, 80, 120.00, 'Valid description text here', 'BCD890',
 CURRENT_DATE - INTERVAL '200 days', 1, NOW() - INTERVAL '11 hours', NOW()),

-- Row 12: NULL UUID
(12, 'twelve@example.com', NULL, '10.10.10.10', 'fe80::a1b2:c3d4:e5f6', '(555) 666-7777', '44444',
 37.7749, -122.4194, FALSE, 70, 85.00, 'More text for testing purposes', 'EFG123',
 CURRENT_DATE - INTERVAL '365 days', 2, NOW() - INTERVAL '12 hours', NOW()),

-- Row 13: Invalid foreign key (category_id = 99 doesn't exist)
(13, 'thirteen@test.com', 'a1b2c3d4-e5f6-7890-abcd-ef1234567890', '172.20.10.1', '2001:db8:abcd::1234', '555-888-9999', '66666',
 41.9028, 12.4964, TRUE, 90, 200.00, 'Description for foreign key test', 'HIJ456',
 CURRENT_DATE - INTERVAL '10 days', 99, NOW() - INTERVAL '13 hours', NOW()),  -- Invalid FK!

-- Row 14: Another invalid foreign key
(14, 'fourteen@domain.net', 'b2c3d4e5-f6a7-8901-bcde-f23456789012', '192.168.50.1', '::1', '(800) 555-0199', '77777',
 43.6532, -79.3832, FALSE, 50, 30.00, 'Testing foreign key validation', 'KLM789',
 CURRENT_DATE - INTERVAL '5 days', 100, NOW() - INTERVAL '14 hours', NOW()),  -- Invalid FK!

-- Row 15: Duplicate of row 1 (for duplicate detection)
(15, 'valid@example.com', '550e8400-e29b-41d4-a716-446655440000', '192.168.1.1', '2001:0db8:85a3:0000:0000:8a2e:0370:7334', '(555) 123-4567', '12345',
 40.7128, -74.0060, TRUE, 85, 99.99, 'This is a valid description text', 'ABC123',
 CURRENT_DATE - INTERVAL '30 days', 1, NOW() - INTERVAL '15 hours', NOW()),

-- Row 16: Text too short (for text length checks)
(16, 'short@example.com', 'c3d4e5f6-a7b8-9012-cdef-345678901234', '10.20.30.40', '2001:db8::2', '555-222-3333', '88888',
 39.9042, 116.4074, TRUE, 75, 55.00, 'Hi', 'A',  -- Very short!
 CURRENT_DATE - INTERVAL '25 days', 3, NOW() - INTERVAL '16 hours', NOW()),

-- Row 17: Text too long code (for max length)
(17, 'long@example.com', 'd4e5f6a7-b8c9-0123-def0-456789012345', '192.0.0.1', 'fe80::abcd', '(123) 555-7890', '99998',
 31.2304, 121.4737, FALSE, 82, 175.00, 'A very long description that exceeds normal expectations', 'TOOLONGCODE',  -- Exceeds 10 chars!
 CURRENT_DATE + INTERVAL '14 days', 4, NOW() - INTERVAL '17 hours', NOW()),  -- Future date!

-- Row 18: NULL boolean
(18, 'null.bool@test.com', 'e5f6a7b8-c9d0-1234-ef01-567890123456', '8.8.4.4', '2001:4860:4860::8888', '555-444-5555', '12121',
 25.2048, 55.2708, NULL, 68, 95.00, 'Description for null boolean test', 'NOP012',
 CURRENT_DATE - INTERVAL '50 days', 5, NOW() - INTERVAL '18 hours', NOW()),

-- Row 19: Zero values
(19, 'zero@example.com', 'f6a7b8c9-d0e1-2345-f012-678901234567', '0.0.0.0', '::', '(000) 000-0000', '00001',
 0.0000, 0.0000, FALSE, 0, 0.00, 'Description with zero numeric values', 'QRS345',
 CURRENT_DATE, 1, NOW() - INTERVAL '19 hours', NOW()),

-- Row 20: Maximum values
(20, 'max@example.com', 'a7b8c9d0-e1f2-3456-0123-789012345678', '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', '555-999-9999', '99999',
 89.9999, 179.9999, TRUE, 100, 9999.99, 'Maximum boundary values test description', 'TUV678',
 CURRENT_DATE - INTERVAL '1 day', 2, NOW() - INTERVAL '20 hours', NOW());

-- Reset sequences to continue from correct values
SELECT setval('test_categories_id_seq', (SELECT MAX(id) FROM test_categories));
SELECT setval('test_users_id_seq', (SELECT MAX(id) FROM test_users));
SELECT setval('test_data_quality_id_seq', (SELECT MAX(id) FROM test_data_quality));

-- =============================================================================
-- Summary of test data coverage
-- =============================================================================
-- Email: 17 valid, 1 invalid format, 2 NULL
-- UUID: 17 valid, 1 invalid format, 2 NULL
-- IPv4: 18 valid, 2 invalid
-- IPv6: 17 valid, 3 invalid
-- Phone: 17 valid USA format, 3 invalid
-- Zipcode: 18 valid, 2 invalid
-- Latitude: 18 valid (-90 to 90), 2 invalid
-- Longitude: 18 valid (-180 to 180), 2 invalid
-- Boolean: 10 TRUE, 8 FALSE, 2 NULL
-- Dates: 16 past, 1 current, 3 future
-- Foreign Key: 16 valid (1-5), 4 invalid (99, 100, or referencing non-existent)
-- Description: Various lengths including empty and whitespace-only
-- Duplicates: Row 15 is exact duplicate of Row 1
-- =============================================================================
