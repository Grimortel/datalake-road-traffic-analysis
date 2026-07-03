# Sources : 
source par api, source par fichier (site comme zenodo...)

# Objectif : 
COnstruire un datalake avec une decompositon en plusieurs zones avec des contraintes techniques.

# Contraintes techniques : 
Pour la zone raw : utiliser blob ou elastic...
Pour la zone staging et curated : utiliser ce que l'on souhaite.
Pour l'orchestration : utiliser airflow, prefect ou kube flow.
Utiliser des endpoints de récupération (get) API gateway pour chaque zone.