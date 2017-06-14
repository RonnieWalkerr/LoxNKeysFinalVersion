#-------------------------------------------------------------------------------
# Name:        Textmining
# Purpose:     De abstracts van alle papers uit onze eigen database halen
#              De eerder gemaakte (dataHalen_dbVullen script) corpus openen als textbestand
#              Textmining met de worden uit de al geopende corpus
#              
#              
# Author:      Ernst, Ronald, Sjors
#              projectgroep 2
#
# Created:     29-05-2017
# Copyright:   (c) Hogeschool van Arnhem en Nijmegen 2017
#-------------------------------------------------------------------------------
import mysql.connector
from nltk.tokenize import word_tokenize
#import dataHalen_dbVullen

def main():
    #dataHalen_dbVullen.main()
    Data = get_Data()
    compare_abskey(Data[0],Data[1])


#onderstaande functie haalt de benodigde data op om textmining uit te voeren.
## Return: een list met de titel en abstract van ieder paper. en een lijst met allen keywords in de database
def get_Data():
    Titstract = []
    Keywords = []
    conn = mysql.connector.connect (host = "localhost",
                        user = "owe8_1617_gr2",
                        passwd = "blaat1234",
                        db = "owe8_1617_gr2")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT Titel,Abstract,PaperID FROM Paper")
        dataResults = cursor.fetchall()
        for row in dataResults:
            Titstract.append(row)
        cursor.execute("SELECT Keyword FROM Keywords")
        keyResults = cursor.fetchall()
        for row in keyResults:
            Keywords.append(row)
    except:
        print ("Error: unable to fecth data")
    cursor.close()
    conn.close()
    return Titstract, Keywords

#Deze functie bekijkt welken keywords er voorkomen in de Titel en abstract per paper. vervolgens links hij de keywords aan dit paper in de database.
# input: paperdata is een list met de titel en abstract van de papers. corpus = een lijst met allen keywords uit de database
def compare_abskey(paperData,corpus):
    keywords = []
    for begrip in corpus:            #zorgt ervoor dat alleen keywords lowercase worden opgeslagen in een list
        begrip = str(begrip).split("'")
        keywords.append(begrip[1].lower())
    for data in paperData:
        conn = mysql.connector.connect(host="localhost",
                                       user="owe8_1617_gr2",
                                       passwd="blaat1234",
                                       db="owe8_1617_gr2")
        cursor = conn.cursor()
        paperID = str(data[2])
        hits = []
        titel = str(data[0]).lower()
        abstract = str(data[1]).lower()
        data = word_tokenize(titel) + word_tokenize(abstract)
        cursor.execute("""SELECT Keywords_idKeywords From Paper_has_Keywords WHERE Paper_PaperID = '%s'""" % \
                       (paperID))#de querry haalt alle bekende verbindingen tussen keys en papers op uit de database

        knownKeys = cursor.fetchall()
        for word in keywords:
            if word in data:
                hits.append(word) #voegt de hits van de keywords op de papers toe in de list.
        for hit in hits:
            add = True
            cursor.execute("""SELECT idKeywords From Keywords WHERE Keyword = '%s'""" % \
                           (hit))# deze querry haalt de keyword ID's van de hits op uit de database
            KWID = cursor.fetchall()
            for known in knownKeys:#deze loop controleert of er al een verbinding bestaat tussen het keyword en de paper
                for kid in KWID:
                    if str(kid) == str(known):
                        add = False
            if add == True:
                KWID = str(KWID).split(",")
                KWID = KWID[0][2:]
                cursor.execute("""INSERT INTO Paper_has_Keywords(Paper_PaperID, Keywords_idKeywords) 
                                        VALUES('%s','%s')""" % \
                               (paperID, KWID)) # deze query maakt de link aan in de database tussen keyword en paper
                conn.commit()
                
main()