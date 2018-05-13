#!/usr/bin/env python
# -*- coding: utf-8 -*-

# REQUIRE PYTHON 3 (FOR lxml module)
# EXECUTE AT THE ROOT OF THE PROJECT
# python3 utilities/resources-xml-to-csv.py

# GOAL OF THE SCRIPT 
# > ALL CUS TRAFFIC XML GOES INTO ONE BIG CSV TO EASE THE PROCESS OF HADOOP

from lxml import etree
import os
import re
import csv

# Algorithm 
# 	foreach xml in ressources directory do:
# 		read xml
# 		foreach arc in xml do:
#			write arc in csv
#		endfor
#	endfor

# READ GML FILE TO EXTRACT ID - NAME
tree = etree.parse("ressources/cus-traffic.gml")
root = tree.getroot()
all_arc = root.findall('pktd:ArcFeature', root.nsmap)
arc_name_byid = {}
for arc in all_arc:
	id = arc.find("id").text
	name = arc.find("name").text
	arc_name_byid[id] = name

# START PROCESSING ARCs
resources = os.listdir("ressources")
pattern = re.compile("^cus-traffic.*\.xml$")

traffic = []

for r in resources:
	filematch = pattern.match(r)
	if filematch:
		try:
			tree = etree.parse("ressources/"+r)
			for arc in tree.xpath("/donnees/TableDonneesArcs/ARC"):
				arr_arc = [
					arc.get("Ident"),					# id
					arc_name_byid[arc.get("Ident")], 	# name
					arc.get("Etat"),					# etat
					arc.get("EtatExp"),					# etat d'exploitation
					arc.get("DMajEtatExp"), 			# derniere maj de l'état d'exploitation 
					arc.get("Debit"), 					# nombre de véhicule les dernières 3min
					arc.get("Taux"), 					# taux associé au trajet
					arc.get("DebitLisse"), 				# nb véhicule lissé sur les dernieres 30min
					arc.get("TauxLisse"), 				# taux lissé sur 30min
					arc.get("VitesseBRP"), 
					r[12:17],							# mm-dd (mois-jour)
					r[18:23]							# hh-mm (heure-minute)
				]
				traffic.append(arr_arc)
		except:
			print("File "+r+" failed to be processed")

csv_traffic = open('ressources/cus-traffic.csv', 'w')  
with csv_traffic:
   writer = csv.writer(csv_traffic)
   writer.writerows(traffic)
