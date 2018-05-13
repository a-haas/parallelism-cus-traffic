# CUS Traffic

## Proposition de sujet

**Jeu de données :** Open data de la ville de Strasbourg (https://www.strasbourg.eu/open-data)

Plus précisément sera utilisé le jeu de données du trafic routier de la CUS, disponible à l'adresse https://www.strasbourg.eu/trafic-routier-eurometropole.

Le jeu de données étant mis à jour toutes les 3 minutes un programme de récupération des données se chargera de récupérer le jeu de données pendant chaque période et ceci sur une période de plusieurs jours (à priori 1 semaine de travail donc du lundi au vendredi).

**Questions initiales :**

Trouver le temps de trajet moyen entre un point A et un point B sur 1 semaine
Identifier les zones les plus lentes à un instant T
Identifier les zones les plus lentes en moyenne sur la semaine ou sur plusieurs jours ou sur un seul jour

## Structuration du projet
Les explications détaillées du projet se situent dans le fichier rapport.md et rapport.pdf.

### Utilities
Dans le dossier `utilities` se trouve les scripts permettant la récupération `collect-dataset` et le traitement des données `ressources-xml-to-csv.py`

### Ressources
Dans `ressources` se trouvent les données de l'api de la CUS qui ont été téléchargées (.xml et .gml) et éventuellement modifiées (.csv). Par soucis de place, le fichier ressources est situé dans `ressources.zip`, merci de le dézipper avant exécution du projet.

### Apps
Dans `apps` se situe chacune des sous applications MapReduce. Pour exécuter une application lancer le script `./cus-traffic.sh`

## Exécution

	unzip ressources.zip
	cd apps/<desired app>
	./cus-traffic.sh

En cas de problème d'exécution du script, essayer les commandes suivantes

	chmod +x cus-traffic.sh
	dos2unix cus-traffic.sh