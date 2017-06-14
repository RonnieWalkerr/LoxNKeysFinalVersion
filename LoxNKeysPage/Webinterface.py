### Tabel maken van database
# Namen: 
#
#
#
#
#

import mysql.connector
from flask import Flask, request, render_template


app = Flask(__name__)
app.config["APPLICATION_ROOT"] = "/home/owe4_bi1a_2/public_html/Sven"


@app.route('/')
def index():
    return render_template("Homepage.html")


@app.route('/table', methods=['POST'])
def table(connection, cursor):
    # CONNECTIE
    connection = mysql.connector.connect (host = "localhost",
                            user = "owe8_1617_gr2",
                            passwd = "blaat1234",
                            db = "owe8_1617_gr2")
    cursor = connection.cursor()
    
    #OPHALEN DATA INGEVOERD ZOEKWOORD 
    zoekwoord = request.form["searchword"]
    tabledata = []
    cursor.execute("SELECT Paper_has_Keywords.Paper_PaperID FROM Paper_has_Keywords INNER JOIN Keywords ON Paper_has_Keywords.Keywords_idKeywords = Keywords.idKeywords WHERE Keyword = '%s' " % (zoekwoord))
    paperIDs = cursor.fetchall()
    for ID in paperIDs:
        rowdata = []
        cursor.execute("SELECT PubMedID, Titel, DatumVanUitgave FROM Paper WHERE PaperID = '%s' " % (ID))
        tripledata = cursor.fetchall()
        rowdata.append(tripledata) 
        cursor.execute("SELECT Auteur.Naam FROM Auteur INNER JOIN Paper_has_Auteur ON Auteur.idAuteur = Paper_has_Auteur.Auteur_idAuteur WHERE Paper_has_Auteur.Paper_PaperID ='%s' " % (ID))
        auteurdata = cursor.fetchall()
        rowdata.append(auteurdata)
        cursor.execute("SELECT Keywords.Keyword FROM Keywords INNER JOIN Paper_has_Keywords ON Keywords.idKeywords = Paper_has_Keywords.Keywords_idKeywords WHERE Paper_PaperID = '%s'" % (ID) )           
        keyworddata = cursor.fetchall()
        rowdata.append(keyworddata)
        tabledata.append(rowdata)
      
    return render_template("Resultpage.html", tabledata=tabledata)
    
#    try:
#        
#        cursor = connection.cursor()
#        query = "SELECT Titel, Naam  FROM Paper JOIN Auteur ON Naam=idAuteur" 
#        cursor.execute(query)
#        data = cursor.fetchall()
#        testdata = ['kinker', 'zemmel', 'frietje', 'arrie', 'sjors', 'janpeter']
#        return data,  render_template("Resultpage.html", data=testdata)
#    except Exception as e:
#        return(str(e))

if __name__ == '__main__':
    app.run()
    

#for row in data:
#    print( row[0])
#
#cursor.close()
#connection.close()
#sys.exit()