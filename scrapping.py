from bs4 import BeautifulSoup
import requests
import pandas as pd

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