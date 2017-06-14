#-------------------------------------------------------------------------------
# Name:        Pubmed -> database
# Purpose:     doorzoeken van pubmed naar papers over lipoxygenase.
#              de pubmedID's, auteurs, datum en abstract van Pubmed ophalen.
#              in een loop de gegevens opslaan in onze eigen database.
#              
#              
# Author:      Ernst, Ronald, Sjors
#              projectgroep 2
#
# Created:     29-05-2017
# Copyright:   (c)Hogeschool van Arnhem en Nijmegen 2017
#-------------------------------------------------------------------------------

#imports voor de applicatie
from Bio import Entrez
from Bio import Medline
import datetime
import mysql.connector

def main():
    info = get_info()
    laatste_update = info[3]
    idlist = search(info)
    db_connectie(idlist,laatste_update)

#de parameters voor de zoekopdracht waarmee pubmed wordt doorzocht.
#de datum waarop het laatst geupdate is wordt uit de corpus gehaald
def get_info():
    term = "((((lipoxygenase OR LOX))) NOT cancer)"     #zoekterm is "lipoxygenase" terwijl er niet
    try:                                                #wordt niet gezocht wordt op "cancer"
        file = open("corpus.txt", "r")                          
        laatste_update = file.readline()
    except IOError: #afvangen als het bestand niet bestaat
        print("bestand niet gevonden")
    laatste_update = laatste_update.split("=")          #datum van de laatste update wordt uit een .txt gehaald
    laatste_update = laatste_update[1]                      #als het script nog niet is uitgevoerd staat daar 1990
    now = datetime.datetime.now()
    volgende_update = str(now.year)                     #volgende update is de datum waarop het script wordt uitgevoerd
    if len(str(now.month)) == 1:                            #deze datum wordt opgeslagen in het corpus.txt
        if len(str(now.day)) == 1:
            volgende_update += "/0" + str(now.month) + "/0" + str(now.day)  #de now() functie geeft de datum maar in een decimaal
        else:                                                               #pubmed heeft twee decimalen nodig
            volgende_update += "/0" + str(now.month) + "/" + str(now.day)
    else:
        volgende_update += "/" + str(now.month) + "/" + str(now.day)
    maxPaper = 1800
    return term, maxPaper, laatste_update, volgende_update 
    
#zoeken op pubmed met de parameters van get_info(), de gevonden pubmedID's 
#worden terug gebracht naar de main() om later gebruikt te worden om de papers op te halen            
def search(info): 
    Entrez.email = "A.N.Other@example.com"
    handle = Entrez.esearch(db="pubmed", term=info[0],
                            mindate= info[2], maxdate= info[3] ,retmax=info[1])
    record = Entrez.read(handle)
    idlist = record["IdList"]
    return idlist

#het ophalen van de Papers op Pubmed met de PubmedID's verkregen in de search() functie
#de resultaten worden teruggebracht naar de db_connectie() functie om daar verwerkt te worden    
def fetch(pubmed_id):
    dblijst = []
    handle = Entrez.efetch(db="pubmed", id=pubmed_id, rettype="medline", retmode="text")
    record = list(Medline.parse(handle))
    dblijst.append(pubmed_id)            #PubmedID
    dblijst.append(record[0]["DA"])      #datum van uitgave
    dblijst.append(record[0]["AU"])      #auteur
    dblijst.append(record[0]["AB"])      #abstract
    dblijst.append(record[0]["TI"])      #titel
    if "OT" in record[0].keys():             #als er keywords bij de paper staan:
        dblijst.append(record[0]["OT"])      #keywords
    return dblijst

#connectie met onze eigen database, met sql querries wordt de database gevult met
#data die in een loop wordt opgehaalt in de fetch() functie
def db_connectie(idlist,update):
    conn = mysql.connector.connect (host = "localhost",
                    user = "owe8_1617_gr2",
                    passwd = "blaat1234",   
                    db = "owe8_1617_gr2")    #connectie maken met onze database
    cursor = conn.cursor()
    for pubid in idlist:             #gaat alle id's langs die zijn gevonden in de search()
        paper = fetch(pubid)   
        pubmedID = paper[0]
        datum = paper[1]
        abstract = paper[3]
        titel = paper[4]

        if len(paper)== 6:
            Keywords = paper[5]
            cursor.execute("""SELECT Keyword FROM Keywords""")
            dbKW = cursor.fetchall()  #ophalen van al bekende Keywords
            addedKW = []
            
            for KW in dbKW:
                KW = str(KW).split("'")
                addedKW.append(KW[1].lower())
              
            for Keyword in Keywords:
                 fillKW = True
                 Keyword = Keyword.replace("'", " ")

                 for word in addedKW:
                    if str(Keyword).lower() == word:
                          fillKW = False
                 if fillKW == True:
                     cursor.execute("""INSERT INTO Keywords(Keyword)
                           VALUES('%s')""" % \
                           ((str(Keyword))))        #gevonden onbekende keywords in de database zetten


        cursor.execute("""SELECT Naam FROM Auteur""")
        dbAuteurs = cursor.fetchall()   #ophalen van al bekende auteurs
        addedAuteurs = []
        for Au in dbAuteurs:
            Au = str(Au).split("'")
            addedAuteurs.append(Au[1])

        cursor.execute("""SELECT PubMedID FROM Paper""")
        dbPID = cursor.fetchall()  # ophalen van al bekende pudmedID's
        addedPID = []
        for PID in dbPID:
            PID = str(PID).split("'")
            addedPID.append(PID[1])
        # deze loop vult de paper entiteit met de gevonden data uit de papers
        fillPaper = True
        for ID in addedPID:
            if pubmedID == ID:
                fillPaper = False
        if fillPaper == True:
           cursor.execute("""INSERT INTO Paper(PubMedID,Abstract,Titel,DatumVanUitgave) 
                    VALUES(%s,%s,%s,%s)""",
                    (str(pubmedID), str(abstract), str(titel), int(datum)))

        #deze vult nog onbekende auteurs
        for auteur in paper[2]:
            fillAU = True
            auteur = auteur.replace("'"," ")
            for x in addedAuteurs:
                if auteur == x:
                    fillAU = False
            if fillAU == True:
                cursor.execute("""INSERT INTO Auteur(Naam)
                           VALUES('%s')""" % \
                           ((str(auteur))))
        conn.commit()
        
        #het linken van de twee entiteiten Paper en Auteur
        for auteur in paper[2]:
            auteur = auteur.replace("'"," ")
            cursor.execute("""SELECT idAuteur From Auteur WHERE Naam = '%s'""" %\
                            (auteur)) #auteur ophalen uit onze database
            auteur_id = cursor.fetchall()
            auteur_id = str(auteur_id).split(",")
            auteur_id = auteur_id[0][2:]

            # haalt al bekende gelinkte papers op aan auteurs.
            cursor.execute("""SELECT Paper_PaperID From Paper_has_Auteur WHERE Auteur_idAuteur = '%s' """% \
                           (auteur_id))
            knownLinks = cursor.fetchall()

            cursor.execute("""SELECT PaperID From Paper WHERE PubMedID = %s""" % \
                           (pubmedID)) #pubmedID's ophalen uit onze database
            paper_id = cursor.fetchall()
            paper_id = str(paper_id).split(",")
            paper_id = paper_id[0][2:]
            add = True
            
            for known in knownLinks:
                known = str(known).split(",")
                known = known[0][2:]
                if known == paper_id:
                    add = False
            if add == True:
                cursor.execute("""INSERT INTO Paper_has_Auteur(Paper_PaperID, Auteur_idAuteur)
                    VALUES('%s','%s')""" %\
                    (paper_id,auteur_id)) #de paperid's samen met de auteurid's in de database zetten
            conn.commit()
    cursor.close()
    conn.close()
main()
