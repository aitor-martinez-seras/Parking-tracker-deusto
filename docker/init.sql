-- Table: public.raw

-- DROP TABLE IF EXISTS public."raw";

CREATE TABLE IF NOT EXISTS public."raw"
(
    id serial,
    date date,
    "time" time with time zone,
    dbs_parking integer,
    general_parking integer,
    PRIMARY KEY (id)
)

TABLESPACE pg_default;