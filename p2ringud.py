import requests
import json
import psycopg2

# PostgreSQL andmebaasi ühenduse loomine
conn = psycopg2.connect(
    dbname="insurance_data",
    user="postgres",
    password="*",
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

# API URL
url = "https://andmed.stat.ee/api/v1/et/stat/RRI01"

# JSON-päring
query = {
    "query": [
        {
            "code": "Elukindlustusliik",
            "selection": {
                "filter": "item",
                "values": [
                    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"
                ]
            }
        }
    ],
    "response": {
        "format": "json-stat2"
    }
}

# Teeb päringu
response = requests.post(url, json=query)

# Kontrollib, kas päring õnnestus
if response.status_code == 200:
    data = response.json()
    
    # Eeldab, et andmed on vastuses 'value' võtme all
    values = data['value']
    years = list(range(2007, 2018))  # Aastad 2007-2017
    types = [
        "Elukindlustus kokku", "Surmajuhtumikindlustus", "Kapitalikogumiskindlustus",
        "..tulumaksusoodustusega kapitalikogumiskindlustus", "Sünni- ja abiellumiskindlustus",
        "Pensionikindlustus", "..tulumaksusoodustusega pensionikindlustus",
        "Investeerimisriskiga elukindlustus", "..tulumaksusoodustusega investeerimisriskiga elukindlustus",
        "Lisakindlustus", "Muu elukindlustus"
    ]
    
    # Sisestab andmed andmebaasi
    for year in years:
        for i, insurance_type in enumerate(types):
            # Eeldab, et igal tüübil on 12 kuud andmeid
            monthly_values = values[i * 12:(i + 1) * 12]
            total_premiums = sum(monthly_values)  # Kogusumma
            
            # Sisestab elukindlustuse tabelisse
            cursor.execute("""
                INSERT INTO life_insurance (year, type, total_premiums, january, february, march, april, may, june, july, august, september, october, november, december)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (year, insurance_type, total_premiums) + tuple(monthly_values))
    
    # Salvestab muudatused
    conn.commit()
    print("Andmed on edukalt sisestatud.")
else:
    print(f"Error: {response.status_code}")

# Sulgeb ühendus
cursor.close()
conn.close()