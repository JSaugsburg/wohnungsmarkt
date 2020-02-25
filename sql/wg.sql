CREATE SCHEMA wg_gesucht;
CREATE SCHEMA gis;
CREATE EXTENSION postgis;
CREATE TABLE gis.cities (
    name character varying(255) NOT NULL,
    insert_date date DEFAULT CURRENT_DATE NOT NULL,
    entries_count integer
);

ALTER TABLE gis.cities OWNER TO sepp;

CREATE TABLE wg_gesucht.images_inserate (
    id integer,
    image bytea
);

ALTER TABLE wg_gesucht.images_inserate OWNER TO sepp;

CREATE TABLE wg_gesucht.inserate (
    inserat_id integer NOT NULL,
    source character varying(255),
    viertel integer,
    titel character varying(255),
    groesse integer,
    miete_gesamt integer,
    miete_kalt integer,
    miete_sonstige integer,
    nebenkosten integer,
    kaution integer,
    abstandszahlung integer,
    zimmer integer,
    url character varying(255),
    verfuegbar boolean,
    insert_date date DEFAULT CURRENT_DATE NOT NULL,
    stadt character varying(255),
    frei_ab date,
    frei_bis date,
    kontakt jsonb,
    adresse jsonb
);

ALTER TABLE wg_gesucht.inserate OWNER TO sepp;

CREATE TABLE wg_gesucht.sources (
    name character varying(255) NOT NULL,
    url character varying(255) NOT NULL,
    insert_date date DEFAULT CURRENT_DATE NOT NULL
);

ALTER TABLE wg_gesucht.sources OWNER TO sepp;

CREATE TABLE gis.viertel (
    city character varying(255) NOT NULL,
    plz integer NOT NULL,
    name character varying(255) NOT NULL,
    insert_date date DEFAULT CURRENT_DATE NOT NULL
);

ALTER TABLE gis.viertel OWNER TO sepp;

CREATE TABLE wg_gesucht.wg_types (
    type_id integer NOT NULL,
    mitbewohner_w integer,
    mitbewohner_m integer,
    insert_date date DEFAULT CURRENT_DATE NOT NULL
);

ALTER TABLE wg_gesucht.wg_types OWNER TO sepp;

CREATE SEQUENCE wg_gesucht.wg_types_type_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE wg_gesucht.wg_types_type_id_seq OWNER TO sepp;

ALTER SEQUENCE wg_gesucht.wg_types_type_id_seq OWNED BY wg_gesucht.wg_types.type_id;

CREATE TABLE wg_gesucht.wohnungs_types (
    type_id integer NOT NULL,
    type character varying(255) NOT NULL,
    insert_date date DEFAULT CURRENT_DATE NOT NULL
);

ALTER TABLE wg_gesucht.wohnungs_types OWNER TO sepp;

CREATE SEQUENCE wg_gesucht.wohnungs_types_type_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE wg_gesucht.wohnungs_types_type_id_seq OWNER TO sepp;

ALTER SEQUENCE wg_gesucht.wohnungs_types_type_id_seq OWNED BY wg_gesucht.wohnungs_types.type_id;

ALTER TABLE ONLY wg_gesucht.wg_types ALTER COLUMN type_id SET DEFAULT nextval('wg_gesucht.wg_types_type_id_seq'::regclass);

ALTER TABLE ONLY wg_gesucht.wohnungs_types ALTER COLUMN type_id SET DEFAULT nextval('wg_gesucht.wohnungs_types_type_id_seq'::regclass);

ALTER TABLE ONLY gis.cities
    ADD CONSTRAINT cities_pkey PRIMARY KEY (name);

ALTER TABLE ONLY wg_gesucht.inserate
    ADD CONSTRAINT inserate_pkey PRIMARY KEY (inserat_id);

ALTER TABLE ONLY wg_gesucht.sources
    ADD CONSTRAINT sources_pkey PRIMARY KEY (name);

ALTER TABLE ONLY gis.viertel
    ADD CONSTRAINT viertel_pkey PRIMARY KEY (plz);

ALTER TABLE ONLY gis.viertel
    ADD CONSTRAINT viertel_name_fkey FOREIGN KEY (city) REFERENCES gis.cities(name);

ALTER TABLE ONLY wg_gesucht.wg_types
    ADD CONSTRAINT wg_types_pkey PRIMARY KEY (type_id);

ALTER TABLE ONLY wg_gesucht.wohnungs_types
    ADD CONSTRAINT wohnungs_types_pkey PRIMARY KEY (type_id);

ALTER TABLE ONLY wg_gesucht.wohnungs_types
    ADD CONSTRAINT wohnungs_types_type_key UNIQUE (type);

ALTER TABLE ONLY wg_gesucht.images_inserate
    ADD CONSTRAINT images_inserate_id_fkey FOREIGN KEY (id) REFERENCES wg_gesucht.inserate(inserat_id);

ALTER TABLE ONLY wg_gesucht.inserate
    ADD CONSTRAINT inserate_source_fkey FOREIGN KEY (source) REFERENCES wg_gesucht.sources(name);

ALTER TABLE ONLY wg_gesucht.inserate
    ADD CONSTRAINT inserate_viertel_fkey FOREIGN KEY (viertel) REFERENCES gis.viertel(name);
