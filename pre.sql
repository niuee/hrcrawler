CREATE ROLE horse_crawler WITH LOGIN PASSWORD 'XXXXXXXXXXXXX';
CREATE SCHEMA IF NOT EXISTS horse;
ALTER SCHEMA horse OWNER TO horse_crawler;
SET ROLE horse_crawler;
CREATE TYPE horse.gender AS ENUM ('Mare', 'Horse', 'Gelding');
CREATE TABLE horse.horses (id VARCHAR(15) PRIMARY KEY, horse_name VARCHAR(255), alt_name VARCHAR(255), horse_gender horse.gender, sire_id VARCHAR(15) REFERENCES horse.horses(id) ON DELETE CASCADE, dam_id VARCHAR(15) REFERENCES horse.horses(id) ON DELETE CASCADE, horse_weight INTEGER, jra_registered BOOLEAN, classic BOOLEAN, active BOOLEAN, born_place VARCHAR(100), born_date DATE,fur_color VARCHAR(100));