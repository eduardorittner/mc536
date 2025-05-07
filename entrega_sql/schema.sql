CREATE TABLE "Pais" (
  "id" int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "nome" varchar UNIQUE
);

CREATE TABLE "Populacao" (
  "id" int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "ano" int NOT NULL,
  "sexo" varchar,
  "faixa_etaria" varchar,
  "quantidade" bigint,
  "pais_id" int NOT NULL
);

CREATE TABLE "Escolaridade" (
  "id" int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "porc_sem_escolaridade" numeric,
  "porc_primario_alcancado" numeric,
  "porc_primario_completo" numeric,
  "porc_secundario_alcancado" numeric,
  "porc_secundario_completo" numeric,
  "porc_superior_alcancado" numeric,
  "porc_superior_completo" numeric,
  "populacao_id" int NOT NULL
);

CREATE TABLE "Media_Estudo" (
  "id" int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "media_anos_primario" numeric,
  "media_anos_secundario" numeric,
  "media_anos_superior" numeric,
  "media_anos_estudo" numeric,
  "populacao_id" int NOT NULL
);

CREATE TABLE "Energia" (
  "id" int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "producao" numeric,
  "consumo" numeric,
  "mudanca_anual_consumo" numeric,
  "mudanca_anual_producao" numeric
);

CREATE TABLE "Eletricidade" (
  "id" int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "producao" numeric,
  "porcentagem" numeric
);

CREATE TABLE "Producao" (
  "id" int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "ano" int,
  "pais_id" int NOT NULL,
  "biocombustivel" int,
  "carvao" int,
  "solar" int,
  "eolica" int,
  "gas" int,
  "combustivel_fossil" int,
  "hidro" int,
  "nuclear" int
);

CREATE TABLE "Fonte" (
  "id" int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "energia" int,
  "eletricidade" int
);

ALTER TABLE "Populacao" ADD FOREIGN KEY ("pais_id") REFERENCES "Pais" ("id");

ALTER TABLE "Escolaridade" ADD FOREIGN KEY ("populacao_id") REFERENCES "Populacao" ("id");

ALTER TABLE "Media_Estudo" ADD FOREIGN KEY ("populacao_id") REFERENCES "Populacao" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("pais_id") REFERENCES "Pais" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("biocombustivel") REFERENCES "Fonte" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("carvao") REFERENCES "Fonte" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("solar") REFERENCES "Fonte" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("eolica") REFERENCES "Fonte" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("gas") REFERENCES "Fonte" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("combustivel_fossil") REFERENCES "Fonte" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("hidro") REFERENCES "Fonte" ("id");

ALTER TABLE "Producao" ADD FOREIGN KEY ("nuclear") REFERENCES "Fonte" ("id");

ALTER TABLE "Fonte" ADD FOREIGN KEY ("energia") REFERENCES "Energia" ("id");

ALTER TABLE "Fonte" ADD FOREIGN KEY ("eletricidade") REFERENCES "Eletricidade" ("id");
