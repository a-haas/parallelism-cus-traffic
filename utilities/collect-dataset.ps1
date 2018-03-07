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

$url = "http://jadyn.strasbourg.eu/GPS/dynn.xml"
$ressources = "..\ressources"
while(1){
    # Format : mois-jour-heure-minutes
    $output = "cus-traffic-$(get-date -f MM-dd-HH-mm).xml"
    # Télecharger et enregistrer les fichiers
    Invoke-WebRequest -Uri $url -OutFile "$ressources\$output")
    # Attendre 3 minutes
    Start-Sleep -s 180
}