```markdown
# 📦 SmartCityAnalytics avec Spark & Scala

Projet académique de Master 1 Intelligence Artificielle (DIT)  
**Étudiant :** Moussa Mallé  
**Email :** mallemoussa091@gmail.com

---

## 📚 Sujet

Développer un système d’analyse de données urbaines (« Smart City ») en utilisant Apache Spark avec Scala.

**Objectifs :**
- Ingestion de données multi-format (CSV, JSON, Parquet)
- Nettoyage et validation des données capteurs et zones
- Enrichissement avancé (features temporelles, météo, densité de trafic)
- Analytique urbaine : indicateurs de congestion, incidents, rétention capteurs
- Optimisations Spark (cache, persist, broadcast)
- Export des résultats en CSV et Parquet

---

## 📁 Structure du projet

```hocon

SmartCityAnalytics/
├── data/                 # Fichiers d’entrée
├── output/               # Résultats finaux
├── src/
│   └── main/
│       └── scala/com/SmartCityAnalytics/analytics/
│           ├── MainApp.scala
│           ├── DataIngestion.scala
│           ├── DataTransformation.scala
│           ├── Analytics.scala
│           └── models/
│               ├── CityZones.scala
│               ├── Sensors.scala
│               ├── TrafficEvents.scala
│               └── Weather.scala
├── application.conf      # Configuration externe (Typesafe Config)
├── build.sbt             # Fichier de build SBT
└── README.md             # Ce document

````

---

## 🛠️ Prérequis
- Java 17  
- Scala 2.12.18  
- Spark 3.5.1  
- SBT  
- Spark SQL / Core

---

## ⚙️ Configuration

Le fichier `application.conf` centralise tous les chemins d'entrée/sortie :

```hocon
app {
  name = "SmartCityAnalytics"
  env = "dev"
  spark {
    master = "local[*]"
    appName = "SmartCityAnalyticsApp"
  }
  data {
    input {
      city_zones     = "data/city_zones.csv"
      sensors        = "data/sensors.csv"
      traffic_events = "data/traffic_events.csv"
      weather        = "data/weather.json"
    }
    output {
      path = "output/results"
    }
  }
}
````

---

## 🚀 Exécution

**Cloner le projet :**

```bash
git clone https://github.com/codeangel223/DataEngineering-SmartCityAnalytics.git SmartCityAnalytics
cd SmartCityAnalytics
```

**Placer les données** dans le dossier `data/`.

**Lancer :**

```bash
sbt run
```

---

## ✨ Fonctionnalités Implémentées

### 📥 Ingestion & Validation

**Lecture multi-format :**

* **CSV** : zones, capteurs, événements de trafic
* **JSON** : météo

**Validation métier :**

* **Zones** : `population > 0`, `surface > 0`
* **Sensors** : coordonnées valides, statut non vide
* **TrafficEvents** : `vitesse >= 0`, `densité ∈ [0,1]`
* **Weather** : température, humidité, pression valides

### 🧠 Transformations Avancées

* **UDF `extractTimeFeatures`** : à partir d’un timestamp → `heure`, `jour`, `mois`, `is_weekend`, `day_period`, `is_working_hour`.
* **Enrichissement** : jointure **événements ↔ capteurs ↔ zones ↔ météo** (mesure la plus proche dans le temps).
* **Niveaux de congestion** : `Low`, `Moderate`, `High`, `Severe`.
* **Indicateurs d’incidents** (flag).
* **Fenêtres temporelles** pour calculs glissants par capteur.

### 📊 Analytique Smart City

#### ✅ KPI par Zone

* Nombre d’événements, véhicules moyens
* Vitesse moyenne, densité moyenne
* Taux d’incidents
* Score de congestion
* Classement des zones par district

#### 📈 KPI par Capteur

* Événements totaux, incidents détectés
* Jours actifs observés (**uptime ratio**)
* Statut attendu vs. observé

#### 📉 Analyse de Cohortes

* Identification du **premier mois d’activité** de chaque capteur
* Taux de **rétention** par mois depuis activation

#### ⏰ Heures de pointe

* Détection de **l’heure la plus chargée** par zone
* Vitesse moyenne associée

### 🚀 Optimisations Spark

* `cache()` et `persist(StorageLevel.MEMORY_AND_DISK_SER)` pour améliorer les perfs
* `broadcast()` sur les petites tables (zones, capteurs)

---

## 💾 Résultats

Les résultats sont enregistrés en double format :

```
output/results/
├── zone_kpi_report/
│   ├── csv/
│   └── parquet/
├── sensor_kpi_report/
├── sensor_cohort_analysis/
└── zone_peak_hours/
```

---

## 📊 Exemple de sorties

**KPI par zone :**

* Volume de trafic
* Vitesse moyenne
* Score de congestion
* Taux d’incidents

**Analyse de cohortes :**

* Mois de premier événement par capteur
* Nombre de capteurs encore actifs après *n* mois

---

## 👨‍🎓 Étudiant

* **Nom :** Moussa Mallé
* **Email :** [mallemoussa091@gmail.com](mailto:mallemoussa091@gmail.com)
* **Formation :** Master 1 Intelligence Artificielle – DIT

---

## 📄 Licence

Projet académique – usage pédagogique uniquement.

```
::contentReference[oaicite:0]{index=0}
```
