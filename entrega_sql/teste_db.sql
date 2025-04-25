CREATE TABLE "Pais" (
  "id" int PRIMARY KEY,
  "nome" varchar,
  "continente" varchar,
  "populacao_total" bigint,
  "ano" int
);

CREATE TABLE "Populacao" (
  "id" int PRIMARY KEY,
  "sexo" varchar,
  "faixa_etaria" varchar,
  "quantidade" bigint,
  "pais_id" int NOT NULL
);

CREATE TABLE "Escolaridade" (
  "id" int PRIMARY KEY,
  "grupo_etario" varchar,
  "primary_attained" numeric,
  "secondary_attained" numeric,
  "tertiary_attained" numeric,
  "pais_id" int NOT NULL
);

CREATE TABLE "Media_Estudo" (
  "id" int PRIMARY KEY,
  "media_anos_estudo" numeric,
  "grupo_etario" varchar,
  "pais_id" int NOT NULL
);

CREATE TABLE "Energia" (
  "id" int PRIMARY KEY,
  "ano" int,
  "producao_total" numeric,
  "consumo_total" numeric,
  "importacao" numeric,
  "demanda" numeric
);

CREATE TABLE "Eletricidade" (
  "id" int PRIMARY KEY,
  "ano" int,
  "producao_total" numeric,
  "consumo_total" numeric,
  "importacao" numeric,
  "demanda" numeric
);

CREATE TABLE "Producao" (
  "id" int PRIMARY KEY,
  "tipo" varchar,
  "unidade" varchar,
  "biocombustivel" numeric,
  "carvao" numeric,
  "solar" numeric,
  "eolica" numeric,
  "gas" numeric,
  "combustivel_fossil" numeric,
  "hidro" numeric,
  "nuclear" numeric,
  "energia_id" int,
  "eletricidade_id" int,
  "pais_id" int NOT NULL
);

CREATE TABLE "Variacao_Consumo" (
  "id" int PRIMARY KEY,
  "tipo" varchar,
  "variacao" numeric,
  "pais_id" int NOT NULL
);

ALTER TABLE "Populacao" ADD FOREIGN KEY ("pais_id") REFERENCES "Pais" ("id");

ALTER TABLE "Escolaridade" ADD FOREIGN KEY ("pais_id") REFERENCES "Pais" ("id");

ALTER TABLE "Media_Estudo" ADD FOREIGN KEY ("pais_id") REFERENCES "Pais" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("energia_id") REFERENCES "Energia" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("eletricidade_id") REFERENCES "Eletricidade" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("pais_id") REFERENCES "Pais" ("id");

ALTER TABLE "Variacao_Consumo" ADD FOREIGN KEY ("pais_id") REFERENCES "Pais" ("id");