#!/usr/bin/env bash

# Projet de parralélisme
# Collecteur de données

# Algorithmique
# Tant que les poules n'ont pas de dents:
#   Télecharger le fichier xml http://jadyn.strasbourg.eu/GPS/dynn.xml
#   Si il y a une erreur (en gros différent d'un statut 200):
#       On averti de l'erreur
#   Sinon
#       On créé le fichier dans le dossier ressources 
#   Attendre 3 minutes
# Fin tant que

url="http://jadyn.strasbourg.eu/GPS/dynn.xml"
ressources="../ressources"
while true
do
	# Format : mois-jour-heure-minutes
    output="$ressources/cus-traffic-$(date +"%m-%d-%H-%M").xml"
	wget -O $output $url
	sleep 180
done