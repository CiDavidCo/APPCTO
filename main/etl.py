# --IMPORT
import pymysql
import csv
import datetime
import os
# --VARIABILI
boolD = True

DB_USERNAME = "anonimo"
DB_PASSWORD = "phpmyadmi#"
DB_HOSTNAME = "localhost"
DB_NAME = "anagrafica_aziende"

PATH_AZIENDE = "csv/aziende.csv"
PATH_STUDENTI = "csv/studenti.csv"
PATH_LOG = "files/log.txt"
PATH_ERROR = "files/error.csv"

# --FUNZIONI

# --CLASSI

if __name__ == "__main__":
    if boolD:
        print("Inzio programma")

        conn = pymysql.connect(DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, DB_NAME)

        f_aziende = open(PATH_AZIENDE, "r")
        f_studenti = open(PATH_STUDENTI, "r")
        #f_error = open(PATH_ERROR, "a")
        # f_log = open(PATH_LOG, "a")

        try:
            csv_reader = csv.reader(f_aziende, delimiter=";")
            cursor = conn.cursor()

            count = 0

            for line in csv_reader:
                if count >= 1:

                    sql_insert = "INSERT INTO Aziende (ID, TIPO_AZIENDA, RAGIONE_SOCIALE, COMUNE, PROVINCIA, STATO,"\
                        " INDIRIZZO, CAP, TELEFONO, EMAIL , SITO, N_DIPENDENTI, DATA_CONVENZIONE, SETTORE,"\
                        " CODICE_ATECO, DESC_ATECO) "\
                        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    data = (line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9],
                            line[10], line[11], line[12], line[13], line[14], line[15])

                    cursor.execute(sql_insert, data)
                    conn.commit()
                count += 1

        except Exception as ex:
            print(ex)
        f_aziende.close()
        f_studenti.close()
        # f_error.close()
        # f_log.close()
        conn.close()

    if boolD:
        print("Fine programma")
