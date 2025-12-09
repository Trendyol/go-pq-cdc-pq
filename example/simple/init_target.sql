-- Create target_user if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'target_user') THEN
        CREATE USER target_user WITH PASSWORD 'target_pass';
    END IF;
END
$$;

-- Create users table
CREATE TABLE IF NOT EXISTS public.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON TABLE public.users TO target_user;
GRANT USAGE, SELECT ON SEQUENCE users_id_seq TO target_user;

