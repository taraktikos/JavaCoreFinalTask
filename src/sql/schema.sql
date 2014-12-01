DROP TABLE IF EXISTS country;
CREATE TABLE country
(
    id SERIAL PRIMARY KEY NOT NULL,
    name VARCHAR(200) NOT NULL,
    code VARCHAR(2) NOT NULL,
    long_code VARCHAR(20) NOT NULL
);
ALTER TABLE country
 ADD CONSTRAINT unique_country_name_code UNIQUE (name, code);
ALTER TABLE country
 ADD CONSTRAINT unique_long_code UNIQUE (long_code);

DROP TABLE IF EXISTS city;
CREATE TABLE city
(
    id SERIAL PRIMARY KEY NOT NULL,
    name VARCHAR(200) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    accuracy INT NOT NULL,
    region_id INT NOT NULL,
    postcode_id INT NOT NULL
);
ALTER TABLE city
 ADD CONSTRAINT unique_name_region_id_postcode_id UNIQUE (name, region_id, postcode_id);

DROP TABLE IF EXISTS region;
CREATE TABLE region
(
    id SERIAL PRIMARY KEY NOT NULL,
    name VARCHAR(200) NOT NULL,
    code VARCHAR(20) NOT NULL,
    country_id INT NOT NULL
);
ALTER TABLE region
 ADD CONSTRAINT unique_region_name_code UNIQUE (name, code);

DROP TABLE IF EXISTS postcode;
CREATE TABLE postcode
(
    id SERIAL PRIMARY KEY NOT NULL,
    name VARCHAR(200) NOT NULL
);
ALTER TABLE postcode
 ADD CONSTRAINT unique_name UNIQUE (name);