# Guide de démarrage complet

---

````markdown
# 🚀 Guide de démarrage – Ecommerce Analytics

Ce projet implémente un pipeline complet d'analyse de données e-commerce avec Apache Spark et Scala.

## 🔧 Prérequis

- Java 11
- Scala 2.11.12
- SBT (Simple Build Tool)
- Apache Spark 2.2.1 installé (si exécution via `spark-submit`)

## 📦 Installation

1. Cloner le projet :
```bash
git clone https://github.com/codeangel223/DataEngineering-EcommerceAnalytics.git ecommerce-analytics
cd ecommerce-analytics
````

2. Vérifier la configuration :
   Le fichier `application.conf` contient les chemins des jeux de données et du dossier de sortie :

```hocon
app {
  data {
    input {
      transactions = "data/transactions.csv"
      users = "data/users.json"
      merchants = "data/merchants.csv"
      products = "data/products.parquet"
    }
    output {
      path = "output/results"
    }
  }
}
```

## ⚙️ Lancer l’application

### Option 1 – Depuis SBT (dev/test)

```bash
sbt run
```

### Option 2 – Depuis le JAR avec Spark

1. Compiler le projet :

```bash
sbt clean assembly
```

2. Lancer le `.jar` :

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master local[*] \
  target/scala-2.11/ecommerce-analytics.jar
```

## 📁 Dossiers importants

| Dossier/Fichier    | Description                              |
| ------------------ | ---------------------------------------- |
| `src/`             | Code source Scala                        |
| `application.conf` | Configuration externalisée               |
| `data/`            | Jeux de données d'entrée (CSV/JSON/etc.) |
| `output/results/`  | Résultats générés (CSV, Parquet)         |
| `TASKS.md`         | Liste des fonctionnalités réalisées      |
| `README.md`        | Présentation générale du projet          |

## 👤 Auteur

* **Nom** : Moussa Mallé
* **Email** : [mallemoussa091@gmail.com](mailto:mallemoussa091@gmail.com)
* **Formation** : Master 1 Intelligence Artificielle – DIT


