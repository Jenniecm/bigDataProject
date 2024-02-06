#!/bin/bash

# Clé d'API
api_key="92824eba88954af3b13a13e3a1943181"

# URL de l'API
api_url="https://api.football-data.org/v4/competitions"

# Mettre le fichier JSON dans le répertoire 'livres' de HDFS
hdfs dfs -put -f "${output_file}" 

# Date du jour
current_date=$(date +"%Y-%m-%d")

# Nom du fichier de sortie
output_file="hdfs://localhost:9870/user/project/datalake2/raw/${current_date}./football.json"

# Effectuer la requête HTTP vers l'API et enregistrer la réponse dans un fichier JSON
curl -X GET "${api_url}" -H "X-Auth-Token: ${api_key}" -o "${output_file}"

