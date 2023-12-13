
DROP DATABASE IF EXISTS data_analysis_ss2;
CREATE DATABASE data_analysis_ss2;
USE data_analysis_ss2;

CREATE TABLE IF NOT EXISTS departamento(
    codigo_departamento INT NOT NULL AUTO_INCREMENT,
    nombre_departamento VARCHAR(50) NOT NULL,
    PRIMARY KEY(codigo_departamento)
);

CREATE TABLE IF NOT EXISTS municipio(
    codigo_municipio INT NOT NULL AUTO_INCREMENT,
    nombre_municipio VARCHAR(50) NOT NULL,
    codigo_departamento INT NOT NULL,
    poblacion INT NOT NULL,
    PRIMARY KEY(codigo_municipio),
    FOREIGN KEY(codigo_departamento) REFERENCES departamento(codigo_departamento)
);

CREATE TABLE IF NOT EXISTS muertes_municipio(
    id_muertes_municipio INT NOT NULL AUTO_INCREMENT,
    codigo_municipio INT NOT NULL,
    fecha DATE NOT NULL,
    muertes INT NOT NULL,
    PRIMARY KEY(id_muertes_municipio),
    FOREIGN KEY(codigo_municipio) REFERENCES municipio(codigo_municipio)
);

CREATE TABLE IF NOT EXISTS general_data_by_fecha(
    id_general_data_by_fecha INT NOT NULL AUTO_INCREMENT,
    fecha DATE NOT NULL,
    casos_nuevos INT NOT NULL,
    casos_acumulados INT NOT NULL,
    muertes_nuevas INT NOT NULL,
    muertes_acumuladas INT NOT NULL,
    muertes_capital INT NOT NULL,
    muertes_interior INT NOT NULL,
    PRIMARY KEY(id_general_data_by_fecha)
);