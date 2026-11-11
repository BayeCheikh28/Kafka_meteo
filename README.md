# Kafka Meteo

Pipeline temps reel avec Kafka, Spark Structured Streaming et HDFS autour des donnees
meteo de l'API Open-Meteo.

## Lancer l'infrastructure

```
docker compose up -d --build
```

Services disponibles :
- JupyterLab : http://localhost:8888
- Spark master : http://localhost:8080
- HDFS : http://localhost:9870

Tous les scripts sont dans `work/`, monte dans les conteneurs sous `/home/jovyan/work`.
Ils se lancent depuis un terminal JupyterLab (menu New > Terminal) ou via
`docker exec -it pyspark_notebook bash`.

## Exercice 1 - Topic et producteur simple

```
python work/producers/create_topic.py
python work/producers/hello_producer.py
```

## Exercice 2 - Consommateur simple

```
python work/consumers/simple_consumer.py
```

## Exercice 3 - Producteur meteo en direct

Interroge l'API Open-Meteo toutes les 30 secondes pour 5 villes et envoie les
resultats dans le topic `weather_stream`.

```
python work/producers/current_weather.py
```

## Exercice 4 - Transformation Spark et alertes

Lit `weather_stream`, calcule les niveaux d'alerte vent/chaleur et ecrit dans
`weather_transformed`.

```
spark-submit work/spark/transform_alerts.py
```

## Exercice 5 - Agregats par fenetre glissante

Fenetre de 1 minute, avance de 30 secondes, watermark de 1 minute. Resultat
affiche dans la console.

```
spark-submit work/spark/aggregates_window.py
```

## Exercice 6 - Stockage HDFS

Consomme `weather_transformed` en continu et ecrit chaque message en JSON Lines
dans `/hdfs-data/{country}/{city}/alerts.json`.

```
python work/consumers/hdfs_writer.py
```

## Exercice 7 - Visualisation

Ouvrir `work/notebooks/visualisation.ipynb` dans JupyterLab pour lire les
fichiers HDFS et afficher les graphiques (temperature, vent, alertes, code
meteo le plus frequent par pays).
