/* ======================================================================== */
/* 1. Datenimport */
/* ======================================================================== */
/* Ziel: Importiere Produktions-, Wartungs- und Sensordaten aus CSV-Dateien */

/* Produktionsdaten */
PROC IMPORT DATAFILE="/home/u64078339/Produktionsdaten_2022_M01.csv" OUT=Produktionsdaten_2022_M01 DBMS=CSV REPLACE; GUESSINGROWS=MAX; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Produktionsdaten_2023_M01.csv" OUT=Produktionsdaten_2023_M01 DBMS=CSV REPLACE; GUESSINGROWS=MAX; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Produktionsdaten_2022_M02.csv" OUT=Produktionsdaten_2022_M02 DBMS=CSV REPLACE; GUESSINGROWS=MAX; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Produktionsdaten_2023_M02.csv" OUT=Produktionsdaten_2023_M02 DBMS=CSV REPLACE; GUESSINGROWS=MAX; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Produktionsdaten_2022_M03.csv" OUT=Produktionsdaten_2022_M03 DBMS=CSV REPLACE; GUESSINGROWS=MAX; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Produktionsdaten_2023_M03.csv" OUT=Produktionsdaten_2023_M03 DBMS=CSV REPLACE; GUESSINGROWS=MAX; RUN;

/* Wartungsdaten */
PROC IMPORT DATAFILE="/home/u64078339/Wartungsdaten_2022_M01.csv" OUT=Wartungsdaten_2022_M01 DBMS=CSV REPLACE; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Wartungsdaten_2023_M01.csv" OUT=Wartungsdaten_2023_M01 DBMS=CSV REPLACE; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Wartungsdaten_2022_M02.csv" OUT=Wartungsdaten_2022_M02 DBMS=CSV REPLACE; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Wartungsdaten_2023_M02.csv" OUT=Wartungsdaten_2023_M02 DBMS=CSV REPLACE; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Wartungsdaten_2022_M03.csv" OUT=Wartungsdaten_2022_M03 DBMS=CSV REPLACE; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Wartungsdaten_2023_M03.csv" OUT=Wartungsdaten_2023_M03 DBMS=CSV REPLACE; RUN;

/* Sensordaten */
PROC IMPORT DATAFILE="/home/u64078339/Sensordaten_2022_M01.csv" OUT=Sensordaten_2022_M01 DBMS=CSV REPLACE; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Sensordaten_2023_M01.csv" OUT=Sensordaten_2023_M01 DBMS=CSV REPLACE; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Sensordaten_2022_M02.csv" OUT=Sensordaten_2022_M02 DBMS=CSV REPLACE; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Sensordaten_2023_M02.csv" OUT=Sensordaten_2023_M02 DBMS=CSV REPLACE; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Sensordaten_2022_M03.csv" OUT=Sensordaten_2022_M03 DBMS=CSV REPLACE; RUN;
PROC IMPORT DATAFILE="/home/u64078339/Sensordaten_2023_M03.csv" OUT=Sensordaten_2023_M03 DBMS=CSV REPLACE; RUN;

/* ======================================================================== */
/* 2. Datenkombination */
/* ======================================================================== */
/* Ziel: Kombiniere historische und aktuelle Daten in Gesamt-Datensätze */

/* Produktionsdaten */
DATA Produktionsdaten_M01; SET Produktionsdaten_2022_M01 Produktionsdaten_2023_M01; RUN;
DATA Produktionsdaten_M02; SET Produktionsdaten_2022_M02 Produktionsdaten_2023_M02; RUN;
DATA Produktionsdaten_M03; SET Produktionsdaten_2022_M03 Produktionsdaten_2023_M03; RUN;
DATA Produktionsdaten; SET Produktionsdaten_M01 Produktionsdaten_M02 Produktionsdaten_M03; RUN;

/* Wartungsdaten */
DATA Wartungsdaten_M01; SET Wartungsdaten_2022_M01 Wartungsdaten_2023_M01; RUN;
DATA Wartungsdaten_M02; SET Wartungsdaten_2022_M02 Wartungsdaten_2023_M02; RUN;
DATA Wartungsdaten_M03; SET Wartungsdaten_2022_M03 Wartungsdaten_2023_M03; RUN;
DATA Wartungsdaten; SET Wartungsdaten_M01 Wartungsdaten_M02 Wartungsdaten_M03; RUN;

/* Sensordaten */
DATA Sensordaten_M01; SET Sensordaten_2022_M01 Sensordaten_2023_M01; RUN;
DATA Sensordaten_M02; SET Sensordaten_2022_M02 Sensordaten_2023_M02; RUN;
DATA Sensordaten_M03; SET Sensordaten_2022_M03 Sensordaten_2023_M03; RUN;
DATA Sensordaten; SET Sensordaten_M01 Sensordaten_M02 Sensordaten_M03; RUN;

/* ======================================================================== */
/* 3. Datenbereinigung */
/* ======================================================================== */
/* Ziel: Entferne Duplikate und fehlerhafte Werte */

/* Entfernen von Duplikaten */
PROC SORT DATA=Produktionsdaten NODUPKEY; BY _ALL_; RUN;
PROC SORT DATA=Wartungsdaten NODUPKEY; BY _ALL_; RUN;
PROC SORT DATA=Sensordaten NODUPKEY OUT=Sensordaten_no_duplicates; BY MaschinenID Datum; RUN;

/* Entfernen fehlerhafter Werte (Sensordaten) */
DATA Sensordaten_Cleaned;
    SET Sensordaten_no_duplicates;
    WHERE 0 <= Temperatur <= 100 AND 0 <= Druck <= 10 AND NOT MISSING(Temperatur) AND NOT MISSING(Druck);
RUN;

/* ======================================================================== */
/* 4. Datenaggregation */
/* ======================================================================== */
/* Ziel: Berechne aggregierte Werte auf Monatsbasis */

/* Sensordaten aggregieren */
PROC SQL;
    CREATE TABLE Sensordaten_Aggregated AS
    SELECT MaschinenID,
           INTNX('month', Datum, 0, 'B') AS Monat FORMAT=MONYY7.,
           MEAN(Temperatur) AS Durchschnittstemperatur,
           MEAN(Druck) AS Durchschnittsdruck
    FROM Sensordaten_Cleaned
    GROUP BY MaschinenID, CALCULATED Monat;
QUIT;

/* Produktionsdaten und Sensordaten kombinieren */
PROC SQL;
    CREATE TABLE Produktions_Sensordaten AS
    SELECT a.MaschinenID, a.Datum, a.Produktionsmenge, 
           b.Temperatur, b.Druck
    FROM Produktionsdaten AS a
    LEFT JOIN Sensordaten AS b
    ON a.MaschinenID = b.MaschinenID AND a.Datum = b.Datum;
QUIT;

/* Produktionsmenge vs. Temperatur (monatlich aggregiert) */
PROC SQL;
    CREATE TABLE monatliche_daten AS
    SELECT MaschinenID, 
           INTNX('month', Datum, 0, 'B') AS Monat FORMAT=MONYY7.,
           MEAN(Produktionsmenge) AS D_Produktionsmenge,
           MEAN(Temperatur) AS Durchschnittstemperatur,
           MEAN(Druck) AS Durchschnittsdruck
    FROM produktions_sensordaten
    GROUP BY MaschinenID, CALCULATED Monat;
QUIT;

/* Aggregation von Sensordaten auf Tagesbasis */
PROC SQL;
    CREATE TABLE sensordaten_M01_aggregated AS
    SELECT MaschinenID,
           Datum,
           MEAN(Temperatur) AS Temperatur,
           MEAN(Druck) AS Druck
    FROM sensordaten_M01
    GROUP BY MaschinenID, Datum;
QUIT;

/* Häufigkeitsanalyse */
PROC SQL;
    CREATE TABLE wartungshäufigkeit AS
    SELECT Wartungsgrund, COUNT(*) AS Anzahl
    FROM wartungsdaten
    GROUP BY Wartungsgrund
    ORDER BY Anzahl DESC;
QUIT;

/* Verfügbarkeit der Maschinen */
PROC SQL;
    CREATE TABLE maschinen_verfügbarkeit AS
    SELECT MaschinenID,
           SUM(Wartungsdauer) AS Gesamte_Wartungszeit FORMAT=8.,
           (SUM(Wartungsdauer)/(24*30)) AS Prozent_Wartung FORMAT=PERCENT8.2
    FROM wartungsdaten
    GROUP BY MaschinenID;
QUIT;

/* Maschinenleistung */
PROC SQL;
    CREATE TABLE maschinen_leistung AS
    SELECT MaschinenID,
           AVG(Effizienz) AS Durchschnittliche_Effizienz FORMAT=8.2,
           AVG(Fehlerquote) AS Durchschnittliche_Fehlerquote FORMAT=8.2
    FROM produktionsdaten
    GROUP BY MaschinenID;
QUIT;

/* ======================================================================== */
/* 5. Analysen */
/* ======================================================================== */
/* Ziel: Führe explorative Analysen durch */

/* Clusteranalyse (Produktionsdaten) */
PROC STANDARD DATA=Produktionsdaten MEAN=0 STD=1 OUT=Produktionsdaten_Scaled;
    VAR Produktionsmenge Fehlerquote Effizienz;
RUN;

PROC FASTCLUS DATA=Produktionsdaten_Scaled MAXCLUSTERS=3 OUT=Produktion_Cluster;
    VAR Produktionsmenge Fehlerquote Effizienz;
RUN;

/* Clusteranalyse (Sensordaten) */
PROC STANDARD DATA=Sensordaten_Aggregated MEAN=0 STD=1 OUT=Sensordaten_Scaled;
    VAR Durchschnittstemperatur Durchschnittsdruck;
RUN;

PROC FASTCLUS DATA=Sensordaten_Scaled MAXCLUSTERS=3 OUT=Sensor_Cluster;
    VAR Durchschnittstemperatur Durchschnittsdruck;
RUN;

/* Ausreißererkennung */
PROC UNIVARIATE DATA=Produktionsdaten; VAR Produktionsmenge Fehlerquote; ID MaschinenID; RUN;
PROC UNIVARIATE DATA=Sensordaten_Cleaned; VAR Temperatur Druck; ID MaschinenID; RUN;

/* Korrelationen */
PROC CORR DATA=Produktionsdaten; VAR Effizienz Fehlerquote; WITH Produktionsmenge; RUN;
PROC CORR DATA=Produktions_Sensordaten; VAR Temperatur Druck Produktionsmenge; RUN;

/* Pivot Tabelle */
PROC TABULATE DATA=produktionsdaten;
    CLASS MaschinenID Datum;
    VAR Produktionsmenge Fehlerquote Verbrauchte_Rohstoffe;
    TABLE MaschinenID,
          (Produktionsmenge Fehlerquote Verbrauchte_Rohstoffe)*(MEAN MAX MIN);
    TITLE "Zusammenfassung der Produktionsdaten nach Maschinen";
RUN;

/* ======================================================================== */
/* 6. Visualisierungen */
/* ======================================================================== */
/* Ziel: Veranschauliche Ergebnisse mit Diagrammen */

/* ------------------------------------------------------------------------ */
/* 1. Boxplots: Verteilung von Daten */
/* ------------------------------------------------------------------------ */
/* Zeigen die Streuung und Medianwerte für Produktions- und Wartungskennzahlen */

/* Produktionsmenge je Maschine */
PROC SGPLOT DATA=produktionsdaten;
    VBOX Produktionsmenge / CATEGORY=MaschinenID;
    TITLE "Boxplot der Produktionsmenge je Maschine";
RUN;

/* Fehlerquote je Maschine */
PROC SGPLOT DATA=produktionsdaten;
    VBOX Fehlerquote / CATEGORY=MaschinenID;
    TITLE "Boxplot der Fehlerquote je Maschine";
RUN;

/* Wartungsdauer je Wartungsgrund */
PROC SGPLOT DATA=wartungsdaten;
    VBOX Wartungsdauer / CATEGORY=Wartungsgrund;
    TITLE "Boxplot der Wartungsdauer je Wartungsgrund";
RUN;

/* ------------------------------------------------------------------------ */
/* 2. Streudiagramme: Zusammenhänge zwischen Variablen */
/* ------------------------------------------------------------------------ */
/* Analyse von Korrelationen und Wechselwirkungen zwischen Variablen */

/* Temperatur vs. Druck (Monat M01) */
PROC SGPLOT DATA=work.sensordaten_2022_m01;
    SCATTER X=Temperatur Y=Druck / GROUP=MaschinenID;
    TITLE "Zusammenhang zwischen Druck und Temperatur";
RUN;

/* Produktionsmenge vs. Verbrauchte Rohstoffe */
PROC SGPLOT DATA=work.produktionsdaten_M01;
    SCATTER X=Produktionsmenge Y=Verbrauchte_Rohstoffe / GROUP=MaschinenID;
    TITLE "Zusammenhang zwischen Produktionsmenge und Verbrauchte Rohstoffe";
RUN;

PROC SGPLOT DATA=monatliche_daten;
    SCATTER X=Durchschnittstemperatur Y=D_Produktionsmenge / GROUP=MaschinenID;
    TITLE "Zusammenhang zwischen Temperatur und Produktionsmenge (Monatlich)";
RUN;

/* Produktionsmenge vs. Druck */
PROC SGPLOT DATA=produktions_sensordaten;
    SCATTER X=Druck Y=Produktionsmenge / GROUP=MaschinenID;
    TITLE "Zusammenhang zwischen Produktionsmenge und Druck";
RUN;

/* ------------------------------------------------------------------------ */
/* 3. Heatmaps: Mustererkennung in großen Datensätzen */
/* ------------------------------------------------------------------------ */
/* Visualisieren die Verteilung und Effizienz basierend auf Produktionskennzahlen */

/* Produktionsmenge und Effizienz nach Maschinen und Zeit */
PROC SGPLOT DATA=work.produktionsdaten_m02;
    HEATMAPPARM X=MaschinenID Y=Verbrauchte_Rohstoffe COLORRESPONSE=Produktionsmenge / COLORMODEL=(lightblue blue);
    TITLE "Zusammenhang zwischen Produktionsmenge und Verbrauchte Rohstoffe";
RUN;

/* Temperatur-Druck-Produktionsmenge */
PROC SGPLOT DATA=produktions_sensordaten;
    HEATMAPPARM X=Temperatur Y=Druck COLORRESPONSE=Produktionsmenge / COLORMODEL=(yellow orange red);
    TITLE "Temperatur-Druck-Produktionsmenge Heatmap";
RUN;

/* ------------------------------------------------------------------------ */
/* 4. Zeitreihenanalysen: Trends und gleitende Durchschnitte */
/* ------------------------------------------------------------------------ */
/* Ziel: Identifizierung von Trends und zyklischen Mustern */

/* Sortieren der Sensordaten für Zeitreihenanalyse */
PROC SORT DATA=sensordaten_M01 NODUPKEY OUT=sensordaten_M01_no_duplicates;
    BY MaschinenID Datum;
RUN;

/* Berechnung der gleitenden Durchschnitte */
PROC EXPAND DATA=sensordaten_M01_aggregated OUT=iot_rolling METHOD=NONE;
    BY MaschinenID;
    ID Datum;
    CONVERT Temperatur=Temp_MA5 / TRANSFORMOUT=(MOVAVE 5);
    CONVERT Druck=Druck_MA5 / TRANSFORMOUT=(MOVAVE 5);
RUN;

/* Visualisierung der geglätteten Temperatur über Zeit */
PROC SGPLOT DATA=iot_rolling;
    SERIES X=Datum Y=Temp_MA5 / GROUP=MaschinenID LINEATTRS=(THICKNESS=2);
    XAXIS LABEL="Monat";
    YAXIS LABEL="Temperatur (Gleitender Durchschnitt)";
    TITLE "Temperatur über Zeit (geglättet)";
RUN;

/* Visualisierung der geglätteten Druckwerte über Zeit */
PROC SGPLOT DATA=iot_rolling;
    SERIES X=Datum Y=Druck_MA5 / GROUP=MaschinenID LINEATTRS=(THICKNESS=2);
    XAXIS LABEL="Monat";
    YAXIS LABEL="Druck (Gleitender Durchschnitt)";
    TITLE "Druck über Zeit (geglättet)";
RUN;

/* Produktionsmengen-Trend über Monate */
PROC SGPLOT DATA=produktionsdaten_M02;
    SERIES X=Datum Y=Produktionsmenge / GROUP=MaschinenID LINEATTRS=(THICKNESS=2);
    XAXIS LABEL="Datum";
    YAXIS LABEL="Produktionsmenge";
    TITLE "Produktionsmengen-Trend über Zeit";
RUN;

/* ------------------------------------------------------------------------ */
/* 5. Histogramme: Verteilungsanalysen */
/* ------------------------------------------------------------------------ */
/* Analyse der Verteilung von Produktions- und Sensordaten */

/* Produktionsmenge */
PROC SGPLOT DATA=produktionsdaten;
    HISTOGRAM Produktionsmenge / BINWIDTH=10 FILLATTRS=(COLOR=BLUE);
    DENSITY Produktionsmenge / TYPE=NORMAL;
    TITLE "Verteilung der Produktionsmenge";
RUN;

/* Temperatur */
PROC SGPLOT DATA=sensordaten;
    HISTOGRAM Temperatur / BINWIDTH=2 FILLATTRS=(COLOR=GREEN);
    DENSITY Temperatur / TYPE=NORMAL;
    TITLE "Verteilung der Temperatur";
RUN;

/* Produktionsmenge je Maschine */
PROC SGPLOT DATA=produktionsdaten;
    HISTOGRAM Produktionsmenge / GROUP=MaschinenID BINWIDTH=20;
    TITLE "Histogramm der Produktionsmenge je Maschine";
RUN;

/* ------------------------------------------------------------------------ */
/* 6. Pareto-Analyse: Häufigkeiten und Prioritäten */
/* ------------------------------------------------------------------------ */
/* Identifizierung der häufigsten Wartungsgründe */

/* Pareto-Diagramm */
PROC SGPLOT DATA=wartungshäufigkeit;
    VBAR Wartungsgrund / RESPONSE=Anzahl CATEGORYORDER=RESPDESC DATALABEL;
    TITLE "Pareto-Diagramm der häufigsten Wartungsgründe";
RUN;

/* ------------------------------------------------------------------------ */
/* 7. Balkendiagramme: Maschinenvergleiche */
/* ------------------------------------------------------------------------ */
/* Analyse von Effizienz und Wartungszeit je Maschine */

/* Balkendiagramm: Verfügbarkeit */
PROC SGPLOT DATA=maschinen_verfügbarkeit;
    VBAR MaschinenID / RESPONSE=Prozent_Wartung DATALABEL;
    YAXIS LABEL="Prozentuale Wartungszeit";
    TITLE "Verfügbarkeit der Maschinen (basierend auf Wartungsdauer)";
RUN;

/* Vergleichsbalkendiagramm */
PROC SGPLOT DATA=maschinen_leistung;
    VBAR MaschinenID / RESPONSE=Durchschnittliche_Effizienz DATALABEL;
    VLINE MaschinenID / RESPONSE=Durchschnittliche_Fehlerquote Y2AXIS LINEATTRS=(THICKNESS=2 COLOR=RED);
    YAXIS LABEL="Effizienz (%)";
    Y2AXIS LABEL="Fehlerquote (%)";
    TITLE "Maschinenvergleich: Effizienz und Fehlerquote";
RUN;