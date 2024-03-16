
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import dumps
import pandas as pd 
from json import loads
import csv
from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
import os
def scrapping():
    url='https://en.wikipedia.org/wiki/List_of_largest_companies_in_the_United_States_by_revenue'
    page=requests.get(url)
    soup=BeautifulSoup(page.text,"html.parser")
    #print(soup)

    table=soup.find_all('table',class_="wikitable sortable")
    #table
    table=soup.find_all('table')[1]
    world_title=table.find_all('th')
    #world_title
    world_table_title=[word.text.strip() for word in world_title]
    world_table_title.insert(0,"id")
    #world_table_title
    df=pd.DataFrame(columns=world_table_title)
    #df
    columns_data=table.find_all('tr')
    #columns_data
    for column in columns_data[1:]:
        row_data = column.find_all('td')
        ind_row_data = [data.text.strip() for data in row_data]
        
        # Ajuster ind_row_data pour qu'il corresponde au nombre de colonnes dans df
        while len(ind_row_data) < len(df.columns):
            ind_row_data.append(None)  # Ajouter None pour les données manquantes
        if len(ind_row_data) > len(df.columns):
            ind_row_data = ind_row_data[:len(df.columns)]  # Garder seulement le nombre requis d'éléments
        
        length = df.shape[0]
        df.loc[length] = ind_row_data
    df.to_csv(r'C:\Users\user\Anaconda-Files\Scrapping_web\companies.csv',index=False)
def produce_to_kafka():

    # Configuration du Kafka Producer
    producer = KafkaProducer(bootstrap_servers=[':9092'], #change ip here
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))

    # Nom du topic Kafka
    topic_name = 'companies'

    # Chemin vers votre fichier CSV
    #csv_file_path = r"C:\\Users\\user\\Anaconda-Files\\Scrapping_web\\companies.csv"
    csv_file_path = os.getenv("CSV_FILE_PATH", "C:\\Users\\user\\Anaconda-Files\\Scrapping_web\\companies.csv")
    # Ouvrir le fichier CSV et lire les lignes
    with open(csv_file_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        #next(csvreader)  # Cette ligne est optionnelle, elle sert à sauter l'en-tête du fichier CSV si présent
        for row in csvreader:
            # Convertir la ligne en une chaîne de caractères séparée par des virgules
            message = ','.join(row)
            
            # Envoyer le message au topic Kafka
            producer.send(topic_name, value=message)
      #  tous les messages sont bien envoyés?
    producer.flush()

    # Fermer le producteur proprement
    producer.close()
def consumer_kafka():
    # Création du consommateur Kafka
    topic_name = 'companies'
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],  # Mettez ici votre serveur Kafka
        auto_offset_reset='earliest',  # Pour lire le topic depuis le début
        group_id='None',  # Spécifiez votre propre group_id
        consumer_timeout_ms=1000  # Arrêter le consommateur après une période d'inactivité (ms)
    )
    # Lire les messages du topic et les stocker dans une liste
    messages = []
    for message in consumer:
        # Decoder le message de bytes en string
        decoded_message = message.value.decode('utf-8')
        messages.append(decoded_message)

    # Fermer le consommateur proprement
    consumer.close()

    # Convertir les messages en DataFrame
    # Supposons que chaque message est une ligne de valeurs séparées par des virgules
    df = pd.DataFrame([x.split(',') for x in messages])
    return df
def simple_cleanning():
    df=consumer_kafka()
    # Définir les noms des colonnes en utilisant la première ligne
    df.columns = df.iloc[0]

    # Supprimer la première ligne (qui est maintenant en index 0 après avoir réassigné les colonnes)
    df = df.drop(df.index[0])

    # Réinitialiser l'index si nécessaire pour que les indices commencent à nouveau à 0
    df = df.reset_index(drop=True)
    # Supprimer les colonnes dont le nom est None
    df = df.loc[:, df.columns.notnull()]
    return df
def load_to_db():
    df=simple_cleanning()
    # Convertir le DataFrame en une liste de dictionnaires
    data = df.to_dict('records')
    # Créer une connexion à MongoDB
    client = MongoClient('localhost', 27017) 

    # Sélectionner la base de données. Si elle n'existe pas, elle sera créée automatiquement lors de l'insertion des données
    db = client['Companies']  

    # Sélectionner la collection. Comme pour la base de données, elle sera créée lors de la première insertion
    collection = db['companies']

    # Insérer les données dans la collection
    collection.insert_many(data)

    print("Les données ont été insérées dans MongoDB.")
