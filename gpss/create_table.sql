-- Greenplum table for KDD Cup intrusion detection data
-- This table stores all network connection records for analysis
-- Table name matches GPSS configuration: kdd_data_demo

DROP TABLE IF EXISTS public.kdd_data_demo;

CREATE TABLE public.kdd_data_demo (
    message_id numeric,
    duration double precision,
    protocol_type text,
    service text,
    flag text,
    src_bytes double precision,
    dst_bytes double precision,
    land double precision,
    wrong_fragment double precision,
    urgent double precision,
    hot double precision,
    num_failed_logins double precision,
    logged_in double precision,
    num_compromised double precision,
    root_shell double precision,
    su_attempted double precision,
    num_root double precision,
    num_file_creations double precision,
    num_shells double precision,
    num_access_files double precision,
    num_outbound_cmds double precision,
    is_host_login double precision,
    is_guest_login double precision,
    count double precision,
    srv_count double precision,
    serror_rate double precision,
    srv_serror_rate double precision,
    rerror_rate double precision,
    srv_rerror_rate double precision,
    same_srv_rate double precision,
    diff_srv_rate double precision,
    srv_diff_host_rate double precision,
    dst_host_count double precision,
    dst_host_srv_count double precision,
    dst_host_same_srv_rate double precision,
    dst_host_diff_srv_rate double precision,
    dst_host_same_src_port_rate double precision,
    dst_host_srv_diff_host_rate double precision,
    dst_host_serror_rate double precision,
    dst_host_srv_serror_rate double precision,
    dst_host_rerror_rate double precision,
    dst_host_srv_rerror_rate double precision,
    intrusion_type text
)
DISTRIBUTED RANDOMLY;

-- Create index for better query performance
CREATE INDEX idx_kdd_data_demo_intrusion_type ON public.kdd_data_demo(intrusion_type);

-- Grant necessary permissions
GRANT ALL ON public.kdd_data_demo TO gpadmin;
