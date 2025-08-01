# Listes des tâches

## Partie 1: CONFIGURATION ET STRUCTURE DU PROJET

- Question 1.1
    - [x] Mise en place de la structure du projet
  
- - Question 1.2
  - *Configurez le fichier build.sbt pour*
  - [x] Spécifier une version de Scala compatible avec Spark (scala: 2.11.12 pour spark: 2.2.1)
  - [x] Ajouter les dépendances de Spark Core et Spark SQL
  - [x] Intégrer la librairie Typesafe Config pour la gestion des configurations
    
- Question 1.3 (Documentation README.md)
  - [] *Rendre le projet facilement exécutable par d'autres développeurs*

## Partie 2: INGESTION ET VALIDATION DES DONNÉES

- Question 2.1: *Ingestion multi-format*
  - *Objectif : Lire et typer les données provenant de différentes sources*
  1. [x] Créer une classe DataIngestion pour centraliser les lectures de données.
  2. [x] Implémenter les méthodes de lecture pour chacun des datasets en utilisant les
     case class pour obtenir des Dataset[T].
  

- Question 2.2 – Validation des données
  - *Objectif : Nettoyer les données en éliminant les valeurs incohérentes*
    - [x] Transactions : amount > 0 et timestamp a 14 caractères.
    - [x] Users : age entre 16 et 100, et income > 0.
    - [x] Products: price > 0 et rating entre 1 et 5
    - [x] Merchants : commission_rate entre 0 et 1.


- Question 2.3 – Gestion d’erreurs et résumé
  - *Objectif : Gérer les erreurs de lecture et fournir un bilan des données.*
    - [x] Capturer et afficher les erreurs (fichier introuvable, structure incorrecte, etc.).
    - [x] Afficher le nombre de lignes lues avant validation
    - [x] Afficher le nombre de lignes valides après validation


## PARTIE 3 : TRANSFORMATIONS AVANCÉES
- Question 3.1 – UDF extractTimeFeatures
  - *Objectif : Extraire des caractéristiques temporelles enrichies à partir d'un timestamp.*
    - [x] Écrivez une UDF (User-Defined Function) qui prend en entrée une chaîne de caractères
      (format yyyyMMddHHmmss) et renvoie une structure de données contenant


- Question 3.2 – Fonction enrichTransactionData
  - *Objectif : Combiner et enrichir les données de plusieurs tables.*
    - [x] OK
  

- Question 3.3 – Analyse par partition Window
  - *Objectif : Utiliser les fonctions de fenêtrage pour détecter des comportements
    complexes.*
    - [x] Enrichissez le DataFrame des transactions pour :
      - Calculer le montant cumulé : Créer une colonne contenant le montant total des
        transactions sur une fenêtre glissante de 7 jours. 
      - Utilisateur Actif : Créez une colonne (1 ou 0) qui indique si un utilisateur a
        effectué une transaction au moins 5 jours sur une période glissante de 7 jours


## PARTIE 4 : ANALYTIQUE BUSINESS
- Question 4.1 – Rapport détaillé par marchand
  - *Objectif : Générer des indicateurs de performance clés (KPI) par marchand.*
    - [x] Calculez les métriques suivantes pour chaque marchand (merchant) :
      - [x] Chiffre d'affaires total, nombre de transactions, et clients uniques. 
      - [x] Montant moyen des transactions. 
      - [x] Son classement par CA dans sa catégorie et sa région (utilisez Window
        functions). 
      - [x] La commission totale perçue. 
      - [x] La répartition des ventes par tranche d’âge des clients


- Question 4.2 – Analyse de cohortes utilisateurs
  - *Objectif : Mesurer la fidélité et la rétention des clients.*
    - [x] Réalisez une analyse de cohortes en :
      - Groupant les utilisateurs par leur mois de première transaction


## PARTIE 5 : OPTIMISATIONS SPARK
- Question 5.1 – Optimisation du stockage
  - *Objectif : Améliorer les performances en gérant l'utilisation de la mémoire.*
    - [x] Implémentez les stratégies d'optimisation suivantes dans votre code :
      - [x] Utiliser cache() pour stocker en mémoire les DataFrame réutilisés
      - [x] Utiliser persist(StorageLevel.MEMORY_AND_DISK_SER) pour les DataFrame
        trop volumineux pour la mémoire seule
      - [x] Utiliser unpersist() pour libérer explicitement le cache lorsque ce n'est plus
        nécessaire.

- Question 5.2 – Optimisation des jointures
  - *Objectif : Réduire les coûts de communication réseau (shuffle) lors des jointures*
    - [x] Utiliser la fonction broadcast() sur les petites tables (comme merchants) lors des
      jointures avec les tables plus volumineuses.

## PARTIE 6 : APPLICATION PRINCIPALE
- Question 6.1 – EcommerceAnalyticsApp
  - *Objectif : Créer une application principale qui orchestre l'ensemble du pipeline.*
    - [x] Créez un object principal qui :
      - [x] Initialise une SparkSession avec les paramètres de configuration externalisés
        (voir Partie 7).
      - Exécute toutes les phases du pipeline dans l'ordre (ingestion, transformation,
        analytique).
      - [x] Affiche tous les résultats dans la console
      - [x] Sauvegarde les résultats finaux aux formats CSV et Parquet dans un répertoire de
        sortie.
      - [x] Implémente une gestion des erreurs globale pour un comportement robuste.

## PARTIE 7 : CONFIGURATION EXTERNALISÉE
- Question 7.1 – Fichier application.conf
  - *Objectif : Séparer la configuration du code.*
    ## Configuration de l'application

```hocon
app {
  name = "EcommerceAnalytics"
  env = "dev"
  spark {
    master = "local[*]"
    appName = "EcommerceAnalyticsApp"
  }
  data {
    input {
      transactions = "data/transactions.csv"
      merchants = "data/merchants.csv"
      users = "data/users.json"
      products = "data/products.parquet"
    }
    output {
      path = "output/results"
    }
  }
}
```

```
OWNER
Name: Moussa Mallé
Email: mallemoussa091@gmail.com
```
