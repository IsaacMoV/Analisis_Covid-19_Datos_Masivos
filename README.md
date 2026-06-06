# COVID-19 Big Data Pipeline 🦠📊

Pipeline de análisis de datos a gran escala sobre el dataset COVID-19 del New York Times (Enero 2020 – Marzo 2023), implementado con **Apache Spark**, **MLlib** y el paradigma **MapReduce**.

---

## Descripción general

Este proyecto implementa un pipeline ETL completo siguiendo la **arquitectura Medallion** (Bronze → Silver → Gold), que procesa más de **3.5 millones de registros** de casos y muertes por COVID-19 en Estados Unidos a nivel nacional, estatal y de condado.

El pipeline incluye:
- Ingesta y validación de datos con esquemas explícitos
- Transformaciones con Window functions distribuidas
- Ingeniería de 28 variables para modelado predictivo
- 3 jobs explícitos de MapReduce sobre la API RDD de Spark
- Comparación de 4 modelos predictivos (Regresión Lineal, Polinomial, ARIMA, Prophet)
- Clustering epidémico de estados con K-Means
- 9 visualizaciones automáticas en PNG

---
## Ejecución
# 1. Clonar el repositorio
git clone https://github.com/tu-usuario/covid-bigdata-project.git
cd covid-bigdata-project

# 2. Clonar los datos del NYT dentro del proyecto
git clone https://github.com/nytimes/covid-19-data.git

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Correr el pipeline
python3 main.py

---
## Estructura del proyecto

```
covid-bigdata-project/
│
├── main.py                  # Punto de entrada — orquesta todo el pipeline
├── requirements.txt         # Dependencias Python
│
├── config/
│   └── settings.py          # Configuración central: rutas, parámetros ML, olas epidémicas
│
├── src/
│   ├── ingest.py            # Stage 1: lectura de CSVs → Bronze zone (Parquet)
│   ├── transform.py         # Stage 2: deltas diarios, rolling avg, CFR → Silver zone
│   ├── features.py          # Stage 3A: ingeniería de 28 variables para ML
│   ├── models.py            # Stage 3B: Regresión Lineal, Polinomial, ARIMA, Prophet
│   ├── mapreduce_jobs.py    # Stage 3C: 3 jobs MapReduce explícitos (API RDD)
│   ├── analytics.py         # Stage 3D: estadísticas descriptivas + K-Means → Gold zone
│   └── visualizations.py   # Stage 4: 9 gráficas PNG con matplotlib
│
├── covid-19-data/           # Dataset fuente del NYT (submodule git)
│   ├── us.csv
│   ├── us-states.csv
│   ├── us-counties-20XX.csv
│   └── ...
│
└── data/
    └── processed/
        ├── bronze/          # Datos crudos en Parquet (post-ingesta)
        ├── silver/          # Datos limpios y enriquecidos
        └── gold/            # Outputs finales: rankings, predicciones, clusters
```

---

## Arquitectura del pipeline

```
CSVs NYT  →  [Stage 1: ingest.py]  →  Bronze (Parquet)
                                            ↓
                      [Stage 2: transform.py]  →  Silver (Parquet)
                                                        ↓
              ┌─────────────────────────────────────────┤
              ↓                  ↓                ↓                ↓
       features.py          models.py     mapreduce_jobs.py   analytics.py
       (28 variables)   (4 modelos ML)   (3 jobs RDD)     (descriptivo + KMeans)
              └─────────────────────────────────────────┤
                                                        ↓
                           [Stage 4: visualizations.py]
                                   9 gráficas PNG
                                   Gold zone (Parquet)
```

---

## Datos fuente

Dataset público del **New York Times** — seguimiento diario de COVID-19 en EE.UU.:

| Archivo | Descripción | Filas aprox. |
|---|---|---|
| `us.csv` | Totales nacionales diarios | 1,158 |
| `us-states.csv` | Por estado, diario | 61,942 |
| `us-counties-20XX.csv` | Por condado, por año | 3,525,161 |
| `rolling-averages/us-states.csv` | Promedios móviles precomputados | 61,942 |
| `rolling-averages/anomalies.csv` | Registro de anomalías del NYT | 2,511 |
| `mask-use/mask-use-by-county.csv` | Encuesta de uso de mascarillas (Jul 2020) | 3,142 |

---

## Requisitos

- Python 3.12+
- Java 11 o 17 (requerido por Spark)
- Apache Spark 4.x (instalado vía pip como `pyspark`)

### Instalación

```bash
# Clonar el repositorio
git clone https://github.com/tu-usuario/covid-bigdata-project.git
cd covid-bigdata-project

# Instalar dependencias
pip install -r requirements.txt

# Verificar Java
java -version
```

### Dependencias principales

```
pyspark>=4.0.0
pyarrow>=14.0.0
pandas>=2.0.0
numpy>=1.24.0
statsmodels>=0.14.4
prophet>=1.1.5
matplotlib>=3.7.0
```

---

## Ejecución

```bash
python3 main.py
```

El pipeline corre todos los stages automáticamente en orden. Al finalizar encontrarás:

- `data/processed/bronze/` — datos crudos en Parquet
- `data/processed/silver/` — datos limpios y transformados
- `data/processed/gold/` — resultados analíticos y predicciones
- `data/figures/` — 9 gráficas PNG

---

## Módulos principales

### `ingest.py` — Bronze zone
- Lee CSVs con esquemas explícitos (evita inferencia costosa)
- Consolida 4 archivos anuales de condados en un único DataFrame
- Valida calidad básica post-carga (conteo de filas, tasa de nulos)
- Persiste en Parquet particionado por estado y año

### `transform.py` — Silver zone
- Convierte totales acumulados → deltas diarios con Window `lag()`
- Calcula promedio móvil de 7 días, tasa de crecimiento y CFR
- Marca filas anómalas cruzando con el registro oficial del NYT
- Filtra excepciones geográficas (NYC, Kansas City, Joplin)
- Enriquece condados con la encuesta de uso de mascarillas

### `features.py` — Ingeniería de variables
28 variables organizadas en 8 familias para modelado predictivo:

| Familia | Variables |
|---|---|
| Señal bruta | `daily_cases`, `log_daily_cases` |
| Suavizado | `rolling_avg_7d`, `rolling_avg_14d`, `ma_ratio_7_14` |
| Momentum | `growth_rate_7d_pct`, `growth_rate_14d_pct`, `momentum_differential` |
| Aceleración | `acceleration_raw`, `acceleration_smooth`, `inflection_point` |
| Mortalidad | `cfr_rolling_14d`, `cfr_delta`, `cfr_lag_corrected_14d` |
| Posición temporal | `days_since_first_case_state`, `log_days_since_first_case` |
| Intensidad de ola | `wave_intensity`, `wave_intensity_zscore` |
| Volatilidad | `rolling_std_7d_cases`, `rolling_cv_7d` |

### `mapreduce_jobs.py` — Jobs MapReduce explícitos
Implementación del paradigma Map → Shuffle/Sort → Reduce sobre la API RDD de Spark:

- **Job 1:** Total de casos acumulados por estado
- **Job 2:** Total de muertes acumuladas por estado
- **Job 3:** Ranking nacional de estados por mortalidad (patrón Sort-by-Value)
- **Demo educativa:** comparación `reduceByKey` (con Combiner) vs `groupByKey` (sin Combiner)

### `models.py` — Modelos predictivos
Forecasting a 7 días con split cronológico estricto:

| Período | Fechas | Filas |
|---|---|---|
| Train | Ene 2020 – Oct 2021 | 87,753 |
| Validation | Nov 2021 – Jun 2022 | 71,412 |
| Test | Jul 2022 – Mar 2023 | 48,520 |

Modelos comparados:

| Modelo | Tipo | Implementación |
|---|---|---|
| Regresión Lineal (Ridge) | Global (todos los estados) | MLlib Pipeline |
| Regresión Polinomial (grado 2) | Global | MLlib + PolynomialExpansion |
| ARIMA(7,1,1) | Por estado | statsmodels + applyInPandas |
| Prophet | Por estado | Meta Prophet + applyInPandas |

### `analytics.py` — Gold zone
- Resumen mensual nacional, rankings de estados, análisis por ola epidémica
- Efecto día de la semana en el reporte (artefacto de infraestructura)
- Correlación uso de mascarillas vs pico de casos (Wave 2)
- Clustering K-Means (k=6) de estados por perfil epidémico

### `visualizations.py` — Gráficas
| Archivo | Descripción |
|---|---|
| `viz1_national_timeline.png` | Timeline nacional con bandas de olas |
| `viz2_state_heatmap.png` | Heatmap intensidad epidémica estado × mes |
| `viz3_wave_comparison.png` | Comparación de casos, muertes y CFR por ola |
| `viz4_cfr_evolution.png` | Evolución del CFR con hitos de vacunación |
| `viz5_weekday_effect.png` | Artefacto de reporte por día de la semana |
| `viz6_ma_crossover_california.png` | Cruce de medias móviles MA7/MA14 |
| `viz7_model_comparison.png` | Comparación de modelos (MAE, RMSE, R²) |
| `viz8_state_clusters.png` | Clusters epidémicos de estados (K-Means) |
| `viz9_county_top20.png` | Top 20 condados por casos acumulados |

---

## Olas epidémicas definidas

| Ola | Período | Variante dominante |
|---|---|---|
| Wave 1 | Ene – Jun 2020 | Original |
| Wave 2 | Jul – Sep 2020 | Original (rebrote verano) |
| Wave 3 | Oct 2020 – Mar 2021 | Alpha |
| Wave 4 | Abr – Jun 2021 | Alpha/Beta |
| Wave 5 | Jul – Nov 2021 | Delta |
| Wave 6 | Dic 2021 – Mar 2022 | Omicron |
| Wave 7 | Abr 2022 – Mar 2023 | Subvariantes Omicron |

---

## Configuración de Spark

El pipeline está configurado para ejecución local pero es compatible con clusters YARN:

```python
"spark.sql.shuffle.partitions": "50"        # calibrado para ~3.5M filas local
"spark.sql.adaptive.enabled": "true"         # AQE: re-optimiza en tiempo de ejecución
"spark.driver.memory": "4g"
"spark.executor.memory": "4g"
"spark.sql.parquet.compression.codec": "snappy"
```

Para cluster YARN: cambiar `shuffle.partitions` a 200-400 y ajustar memoria según los ejecutores disponibles.

---

## Tecnologías utilizadas

| Tecnología | Uso |
|---|---|
| Apache Spark 4.x | Motor de procesamiento distribuido |
| PySpark MLlib | Modelos ML (GBT, K-Means, Regresión) |
| Spark RDD API | Jobs MapReduce explícitos |
| statsmodels | ARIMA por estado |
| Prophet (Meta) | Forecasting con changepoints |
| pandas / numpy | Procesamiento local post-collect |
| matplotlib | Generación de visualizaciones |
| Parquet + Snappy | Formato de almacenamiento columnar |

---

## Fuente de datos

New York Times COVID-19 Data:
> The New York Times. (2021). Coronavirus (Covid-19) Data in the United States. Retrieved from https://github.com/nytimes/covid-19-data


