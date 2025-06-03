# -*- coding: utf-8 -*-
"""
Created on Tue Mar  4 11:31:36 2025

@author: DiMartino
"""
import pandas as pd
import duckdb

conn = duckdb.connect(r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\pbix\cockpit.db")

try:
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_idDato START 1;")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_idValore START 1;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS EventoDim (
            idDato INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_idDato'),
            idPipeline INTEGER,
            nomePipeline VARCHAR,
            descPipeline VARCHAR,
            idEvento INTEGER,
            nomeEvento VARCHAR,
            unita VARCHAR,
            isPercentuale BOOL,
            UNIQUE(idPipeline,idEvento)
        )
        """
    )
    
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS TipoVeicoloDim (
            idTipoVeicolo INTEGER PRIMARY KEY,
            nomeTipoVeicolo VARCHAR
        )
        """
    )
    

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS DataDim (
            idData INTEGER PRIMARY KEY,
            anno INTEGER,
            mese INTEGER,
            giorno INTEGER,
            quarter VARCHAR,
            dataCompleta DATE
        )
        """
    )
    

    
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Dati (
            idValore INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_idValore'),
            idDato INTEGER,
            idData INTEGER,
            idTipoVeicolo INTEGER DEFAULT 2,
            valore FLOAT,
            latest BOOL,
            FOREIGN KEY (idDato) REFERENCES EventoDim(idDato),
            FOREIGN KEY (idData) REFERENCES DataDim(idData),
            FOREIGN KEY (idTipoVeicolo) REFERENCES TipoVeicoloDim(idTipoVeicolo),
            UNIQUE(idDato, idData, idTipoVeicolo)
        )
        """
    )
  


    # conn.execute(
    #     """
    #     CREATE TABLE IF NOT EXISTS IamEventoDim (
    #         idPipeline INTEGER,
    #         nomePipeline VARCHAR,
    #         descPipeline VARCHAR,
    #         idEvento INTEGER,
    #         nomeEvento VARCHAR,
    #         tipoEvento VARCHAR,
    #         unita VARCHAR,
    #         isPercentuale BOOL,
    #         PRIMARY KEY(idPipeline, idEvento)
    #     )
    #     """
    # )
    # Big mistake: IamEventoDim had to have an eventID, as did EventoDim, otherwise why have I made all of this? Time to reflect on my choices and mistakes.
    # conn.execute(
    #     """
    #     CREATE TABLE IF NOT EXISTS IamDati (
    #         idPipeline INTEGER,
    #         idEvento INTEGER,
    #         idData INTEGER,
    #         idTipoVeicolo INTEGER,
    #         valore FLOAT,
    #         latest BOOL,
    #         PRIMARY KEY (idPipeline, idEvento, idData, idTipoVeicolo),
    #         FOREIGN KEY (idPipeline, idEvento) REFERENCES EventoDim(idPipeline, idEvento),
    #         FOREIGN KEY (idData) REFERENCES DataDim(idData),
    #         FOREIGN KEY (idTipoVeicolo) REFERENCES TipoVeicoloDim(idTipoVeicolo)
    #     )
    #     """
    # )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS AlimentazioneDim (
            idAlimentazione INTEGER,
            codAlimentazione VARCHAR,
            nomeAlimentazione VARCHAR,
            PRIMARY KEY(idAlimentazione)
        )
        """
    )
    
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS MercatoDim (
            idMercato INTEGER,
            nomeMercato VARCHAR,
            PRIMARY KEY(idMercato)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ImmDati (
            idData INTEGER,
            idAlimentazione INTEGER,
            idMercato INTEGER,
            valore INTEGER,
            latest BOOL,
            PRIMARY KEY (idData, idAlimentazione, idMercato),
            FOREIGN KEY (idAlimentazione) REFERENCES AlimentazioneDim(idAlimentazione),
            FOREIGN KEY (idData) REFERENCES DataDim(idData),
        )
        """
    )

except Exception as e:
    print(e)
    pass
    #%%

conn = duckdb.connect(r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\pbix\cockpit.db")
event_dimension_list = [
    (1, "Istat", "Dati recuperati dal sito ISTAT", 1, "PIL", "Miliardi", 0),
    (1, "Istat", "Dati recuperati dal sito ISTAT", 2, "CrescitaPIL", "Percentuale", 1),
    (1, "Istat", "Dati recuperati dal sito ISTAT", 3, "Disoccupazione", "Percentuale", 1),
    (1, "Istat", "Dati recuperati dal sito ISTAT", 4, "Inflazione", "Percentuale", 1),
    (1, "Istat", "Dati recuperati dal sito ISTAT", 5, "FiduciaImprese", "Indice", 0),
    (2, "Bce", "Dati recuperati dalla Banca Centrale Europea", 6, "TassiDiInteresse", "Percentuale", 1),
    (1, "Istat", "Dati recuperati dal sito ISTAT", 7, "PrezzoBeniEnergetici", "Percentuale", 1),
    (1, "Istat", "Dati recuperati dal sito ISTAT", 8, "PrezzoCarburanteTrasporti", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 9, "DeltaPrezzoMedioPuntuale", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 10, "DeltaPrezzoMedioYtd", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 11, "DeltaPrezzoMedioRolling", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 12, "DeltaFatturatoRolling", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 13, "EffettoVolumiRolling", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 14, "EffettoMixCPRolling", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 15, "EffettoPrezziRolling", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 16, "DeltaFatturatoPuntuale", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 17, "EffettoVolumiPuntuale", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 18, "EffettoMixCPPuntuale", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 19, "EffettoPrezziPuntuale", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 20, "DeltaFatturatoProgressivo", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 21, "EffettoVolumiProgressivo", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 22, "EffettoMixCPProgressivo", "Percentuale", 1),
    (0, "IAM","Dati recuperati dal file IAM", 23, "EffettoPrezziProgressivo", "Percentuale", 1),
    (3, "IMF", "Dati recuperati dalle API International Monetary Fund", 24, "PrezzoOro", "Miliardi", 0),
    (3, "IMF", "Dati recuperati dalle API International Monetary Fund", 25, "PrezzoRame", "Miliardi", 0),
    (3, "IMF", "Dati recuperati dalle API International Monetary Fund", 26, "PrezzoArgento", "Miliardi", 0),
    (3, "IMF", "Dati recuperati dalle API International Monetary Fund", 27, "PrezzoGasolio", "Miliardi", 0),
    (3, "IMF", "Dati recuperati dalle API International Monetary Fund", 28, "PrezzoPetrolio", "Miliardi", 0),
    (3, "IMF", "Dati recuperati dalle API International Monetary Fund", 29, "PrezzoAlluminio", "Miliardi", 0),
    (3, "IMF", "Dati recuperati dalle API International Monetary Fund", 30, "PrezzoGasNaturale", "Miliardi", 0),
    (3, "IMF", "Dati recuperati dalle API International Monetary Fund", 31, "PrezzoFerroGrezzo", "Miliardi", 0),
    (3, "IMF", "Dati recuperati dalle API International Monetary Fund", 32, "PrezzoLitio", "Miliardi", 0),
    (3, "IMF", "Dati recuperati dalle API International Monetary Fund", 33, "PrezzoCobalto", "Miliardi", 0),
    (3, "IMF", "Dati recuperati dalle API International Monetary Fund", 34, "PrezzoTerreRare", "Miliardi", 0),
    (4, "Drewry", "Dati del WCI (World Container Index) recuperati manualmente dal sito Drewry", 35, "TotaleIndiceWCI", "Dollari", 0),
    (4, "Drewry", "Dati del WCI (World Container Index) recuperati manualmente dal sito Drewry", 36, "RotterdamIndiceWCI", "Dollari", 0),
    (4, "Drewry", "Dati del WCI (World Container Index) recuperati manualmente dal sito Drewry", 37, "LosAngelesIndiceWCI", "Dollari", 0),
    (4, "Drewry", "Dati del WCI (World Container Index) recuperati manualmente dal sito Drewry", 38, "GenovaIndiceWCI", "Dollari", 0),
    (4, "Drewry", "Dati del WCI (World Container Index) recuperati manualmente dal sito Drewry", 39, "NewYorkIndiceWCI", "Dollari", 0)
    
    
    ]

mercato_list = [
    ("01", "AUTOBUS"),
    ("02", "AUTOCARRI"),
    ("03", "RIMORCHI LEGGERI"),
    ("04", "RIMORCHI PESANTI"),
    ("05", "SEMIRIMORCHI"),
    ("06", "VEICOLI COMMERCIALI LEGGERI"),
    ("07", "AUTOVETTURE"),
    ("99", "SCONOSCIUTO")
]

alimentazione_list = [
    (0, "B", "BENZINA"),
    (1, "B-E", "HEV (B)"),
    (2, "B-O", "B/OLIO"),
    (3, "B-W", "B/WANK"),
    (4, "BET", "BENZINA-ETANOLO"),
    (5, "BGE", "HEV (B-GPL)"),
    (6, "ELE", "BEV"),
    (7, "G", "DIESEL"),
    (8, "G-E", "HEV (D)"),
    (9, "GBI", "BIODIESEL"),
    (10, "GNL", "GNL"),
    (11, "GPL", "GPL"),
    (12, "H-E", "IDROGENO"),
    (13, "M-E", "IBRIDO METANO/ELETTRICO"),
    (14, "ME", "METANO"),
    (15, "MSC", "MISCELA"),
    (16, "P", "PETROLIO"),
    (17, "PH", "PHEV"),
    (18, "PHI", "PHEV IDROGENO"),
    (99, "ND", "NON DEFINITO")
]

type_dimension_list = [("Autovetture", 0),
                       ("Autocarri", 1),
                       ("Non applicabile", 2)
                       ]

month_mapping = {
    1: "Q1", 2: "Q1", 3: "Q1",
    4: "Q2", 5: "Q2", 6: "Q2",
    7: "Q3", 8: "Q3", 9: "Q3",
    10: "Q4", 11: "Q4", 12: "Q4"
}

date_dimension_list = pd.date_range(start="1990-01-01", end="2039-12-31", freq="D")
date_dimension_list = date_dimension_list.date.tolist()

date_df = pd.DataFrame(date_dimension_list, columns=["dataCompleta"])
date_df["dataCompleta"] = pd.to_datetime(date_df["dataCompleta"])
date_df["anno"] = date_df["dataCompleta"].dt.year
date_df["mese"] = date_df["dataCompleta"].dt.month
date_df["giorno"] = date_df["dataCompleta"].dt.day
date_df["idData"] = date_df["anno"].astype(str) + date_df["mese"].astype(str).str.zfill(2) + date_df["giorno"].astype(str).str.zfill(2)
date_df["quarter"] = date_df["mese"].map(month_mapping).astype(str)

try:
    for index, row in date_df.iterrows():
        conn.execute(
            """
            INSERT INTO DataDim (idData, anno, mese, giorno, quarter, dataCompleta)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT (idData)
            DO NOTHING
            """,
            (row["idData"], row["anno"], row["mese"], row["giorno"], str(row["quarter"]), row["dataCompleta"])
            )
except Exception as e:
    print(e)
    
try:
    conn.executemany("""
                     INSERT INTO EventoDim (idPipeline, nomePipeline, descPipeline, idEvento, nomeEvento, unita, isPercentuale) 
                     VALUES (?,?,?,?,?,?,?)
                     ON CONFLICT (idPipeline, idEvento)
                     DO NOTHING
                                  """, event_dimension_list
                     )
except Exception as e:
    print(e)
     

try:
    conn.executemany("""
                     INSERT INTO TipoVeicoloDim(nomeTipoVeicolo, idTipoVeicolo)
                     VALUES (?,?)
                     ON CONFLICT (idTipoVeicolo)
                     DO NOTHING
                     """, type_dimension_list)
except Exception as e:
    print(e)
    
try:
    conn.executemany("""
                     INSERT INTO AlimentazioneDim(idAlimentazione, codAlimentazione, nomeAlimentazione)
                     VALUES (?,?,?)
                     ON CONFLICT (idAlimentazione)
                     DO NOTHING
                     """, alimentazione_list)
except Exception as e:
    print(e)
    
try:
    conn.executemany("""
                     INSERT INTO MercatoDim(idMercato, nomeMercato)
                     VALUES (?,?)
                     ON CONFLICT (idMercato)
                     DO NOTHING
                     """, mercato_list)
except Exception as e:
    print(e)
    
conn.close()

#%%

conn = duckdb.connect(r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\pbix\cockpit.db")

# conn.execute(
#     """
#     DROP VIEW importBI
#     """
#     )

conn.execute(
    """
    CREATE VIEW IF NOT EXISTS importBI AS
    SELECT
        Dati.idDato AS idDato,
        Dati.idData AS idData, 
        Dati.idTipoVeicolo as idTipoVeicolo,
        Dati.valore AS valore,
        DataDim.anno AS anno,
        DataDim.mese AS mese,
        DataDim.giorno AS giorno,
        DataDim.quarter AS quarter,
        DataDim.dataCompleta AS dataCompleta,
        EventoDim.nomePipeline AS nomePipeline,
        EventoDim.descPipeline AS descPipeline,
        EventoDim.nomeEvento AS nomeEvento,
        EventoDim.unita AS unita,
        EventoDim.isPercentuale AS isPercentuale,
        Dati.latest AS isLatest
    FROM Dati
    INNER JOIN DataDim ON DataDim.idData = Dati.idData
    INNER JOIN EventoDim ON EventoDim.idDato = Dati.idDato
    """
)

# conn.execute(
#     """
#     DROP VIEW fact_Dati
#     """
#     )

conn.execute(
    """
    CREATE VIEW IF NOT EXISTS fact_Dati AS
    SELECT
        Dati.idValore as idValore,
        Dati.idDato as idDato,
        Dati.idData AS idData,
        Dati.idTipoVeicolo as idTipoVeicolo,
        CAST(Dati.valore AS FLOAT) AS valore,
        Dati.latest AS isLatest
    FROM Dati
    """
)

# conn.execute(
#     """
#     DROP VIEW fact_IamDati
#     """
#     )

# conn.execute(
#     """
#     CREATE VIEW IF NOT EXISTS fact_IamDati AS
#     SELECT
#         IamDati.idPipeline AS idPipeline,
#         IamDati.idEvento AS idEvento,
#         IamDati.idData AS idData,
#         IamDati.idTipoVeicolo AS idTipoVeicolo,
#         CAST(IamDati.valore AS FLOAT) AS valore,
#         IamDati.latest AS isLatest
#     FROM IamDati
#     """
# )

# conn.execute(
#     """
#     DROP VIEW dimension_Data
#     """
#     )

conn.execute(
    """
    CREATE VIEW IF NOT EXISTS dimension_Data AS
    SELECT
        DataDim.idData AS idData,
        DataDim.anno AS anno,
        DataDim.mese AS mese,
        DataDim.giorno AS giorno,
        DataDim.quarter AS quarter,
        DataDim.dataCompleta AS dataCompleta
    FROM DataDim
    """
)

# conn.execute(
#     """
#     DROP VIEW dimension_TipoVeicolo
#     """
#     )

conn.execute(
    """
    CREATE VIEW IF NOT EXISTS dimension_TipoVeicolo AS
    SELECT
        TipoVeicoloDim.idTipoVeicolo,
        TipoVeicoloDim.nomeTipoVeicolo
    FROM TipoVeicoloDim
    """
)

# conn.execute(
#     """
#     DROP VIEW dimension_Evento
#     """
#     )

conn.execute(
    """
    CREATE VIEW IF NOT EXISTS dimension_Evento AS
    SELECT
        EventoDim.idDato,
        EventoDim.idPipeline AS idPipeline,
        EventoDim.nomePipeline AS nomePipeline,
        EventoDim.descPipeline AS descPipeline,
        EventoDim.idEvento AS idEvento,
        EventoDim.nomeEvento AS nomeEvento,
        EventoDim.unita AS unita,
        EventoDim.isPercentuale AS isPercentuale
    FROM EventoDim

    """
)

conn.execute(
    """
    CREATE VIEW IF NOT EXISTS fact_Immatricolazioni AS
    SELECT
        idData,
        idMercato,
        idAlimentazione,
        valore,
        latest
    FROM ImmDati
    """
    )
conn.execute(
    """
    CREATE VIEW IF NOT EXISTS dimension_Alimentazione AS
    SELECT
        idAlimentazione,
        codAlimentazione,
        nomeAlimentazione
    FROM AlimentazioneDim
    """
    )
conn.execute(
    """
    CREATE VIEW IF NOT EXISTS dimension_Mercato AS
    SELECT
        idMercato,
        nomeMercato
    FROM MercatoDim
    """
    
)

conn.close()
# import duckdb
# import pandas as pd

# conn = duckdb.connect(r"L:\01.Dati\04.Varie\08.Cockpit\cockpit.db")

# query = "SELECT * FROM ImportBI"
# df = conn.execute(query).fetchdf()

# 

# df

# import duckdb
# import pandas as pd

# conn = duckdb.connect(r"L:\01.Dati\04.Varie\08.Cockpit\cockpit.db")

# query = "SELECT * FROM fact_Dati"
# Fact = conn.execute(query).fetchdf()
# query = "SELECT * FROM dimension_Evento"
# Evento = conn.execute(query).fetchdf()
# query = "SELECT * FROM dimension_Data"
# Data = conn.execute(query).fetchdf()
# # conn.close()
# import duckdb
# import pandas as pd

# conn = duckdb.connect(r"L:\01.Dati\04.Varie\08.Cockpit\cockpit.db")


# query_fact = "SELECT * FROM fact_Dati"
# Fact = conn.execute(query_fact).fetchdf()

# query_evento = "SELECT * FROM dimension_Evento"
# Evento = conn.execute(query_evento).fetchdf()

# query_data = "SELECT * FROM dimension_Data"
# Data = conn.execute(query_data).fetchdf()

# conn.close()