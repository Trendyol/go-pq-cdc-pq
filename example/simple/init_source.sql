-- Create source_user if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'source_user') THEN
        CREATE USER source_user WITH PASSWORD 'source_pass';
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

-- Set replica identity to DEFAULT (uses primary key for updates/deletes)
ALTER TABLE public.users REPLICA IDENTITY DEFAULT;

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON TABLE public.users TO source_user;
GRANT USAGE, SELECT ON SEQUENCE users_id_seq TO source_user;

