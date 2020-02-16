# --IMPORT
import pymysql
import csv
import datetime
import os
import re
# --VARIABILI
boolD = True

DB_USERNAME = "localhost"
DB_PASSWORD = ""
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
        f_error = open(PATH_ERROR, "a")
        f_log = open(PATH_LOG, "a")
        
        count = 0
        error_count = 0
        id_aziende = []
        # Dati aziende
        try:
            csv_reader = csv.reader(f_aziende, delimiter=";")
            cursor = conn.cursor()
            for line in csv_reader:
                if count >= 1:

                    for element in line:
                        if element == '':
                            element = None

                    if re.search('[/D]', line[7]):
                        line[7] = None

                    if re.search('[/D]', line[8]):
                        line[8] = None
                count += 1
            f_aziende.close()
            f_aziende = open(PATH_AZIENDE, "r")
            csv_reader = csv.reader(f_aziende, delimiter=";")
            count = 0
            for line in csv_reader:
                if count >= 1:
                    id_aziende.append((line[0], line[2]))
                    sql_insert = "INSERT INTO Aziende (ID, TIPO_AZIENDA, RAGIONE_SOCIALE, COMUNE, PROVINCIA, STATO," \
                                 " INDIRIZZO, CAP, TELEFONO, EMAIL , SITO, N_DIPENDENTI, DATA_CONVENZIONE, SETTORE," \
                                 " CODICE_ATECO, DESC_ATECO) " \
                                 " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    data = (line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9],
                            line[10], line[11], line[12], line[13], line[14], line[15])
                    # cursor.execute(sql_insert, data)
                    # conn.commit()

                count += 1

        except Exception as ex:
            
            for line in f_aziende:
                f_error.write(line)
                error_count += 1
                print(ex)
                conn.rollback()

        new_array = []
        for t in id_aziende:
            # print(t)
            new_array.append(t)
        # Dati studenti
        try:
            csv_reader = csv.reader(f_studenti, delimiter=";")
            cursor = conn.cursor()

            id_count = 0
            
            for line in csv_reader:
                if id_count >= 1:
                    id_count += 1
                    sql_insert = "INSERT INTO Studenti (ID, CLASSE, RUOLO, DESCRIZIONE, ID_AZIENDA"\
                        " VALUES (%s, %s, %s, %s, %s)"

                    for t in new_array:
                        print(t)
                    data = (id_count, line[1], line[2], line[6], line[7])

                    # cursor.execute(sql_insert, data)
                    # conn.commit()
                    # count += 1
        except Exception as ex:
            
            for line in f_studenti:
                f_error.write(line)
                error_count += 1
                print(ex)
                conn.rollback()
        
        f_aziende.close()
        f_studenti.close()
        conn.close()

        f_log.write("Ora dell'esecuzione: " + str(datetime.datetime.now()) + "\n" + "Programma eseguito: " +
                    str(os.path.basename(__file__)) + "\n" + "File in input: (" + PATH_AZIENDE + ", " + PATH_STUDENTI +
                    ", " + PATH_ERROR + ", " + PATH_LOG + ")\n" + "Nome tabelle aggiornate:" +
                    "aziende, studenti\n" + "Numero di record aggiornati: " + str(count) + "\n" +
                    "Totale record scritti sul file error: " + str(error_count) + "\n\n")
        
        f_error.close()
        f_log.close()

    if boolD:
        print("Fine programma")
