-- Create target_user if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'target_user') THEN
        CREATE USER target_user WITH PASSWORD 'target_pass';
    END IF;
END
$$;

-- Create user_profiles table with different schema than source
CREATE TABLE IF NOT EXISTS public.user_profiles (
    user_id INTEGER PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    email_address VARCHAR(255),
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON TABLE public.user_profiles TO target_user;

