# Observatorio y Pipeline de Ciencia de Datos — Accidentalidad Vial Colombia (`yb9r-2dsi`)

Proyecto Final de Ciencia de Datos y Minería de Datos en Python conectado al portal oficial de **Datos Abiertos Colombia** (`https://www.datos.gov.co/api/v3/views/yb9r-2dsi/query.json`).

## Estructura del Repositorio

- **`conexion_api.py`**: Pipeline modularizado en Python que implementa las 4 fases obligatorias:
  - **Fase I:** Extracción paginada secuencial con variables dinámicas (`$limit`, `$offset`), bucle `while` y consolidación masiva con bucle `for` en lista y diccionario estructurado.
  - **Fase II (`Pandas` & `NumPy`):** Concatenación eficiente con `pd.concat()`, tratamiento explícito de valores faltantes (`NaN`) y duplicados (`drop_duplicates`), casteo explícito a tipos numéricos (`int64`, `float64`), cálculo vectorizado de severidad y filtrado avanzado anidado con operadores lógicos (`&`, `|`) agrupados entre paréntesis.
  - **Fase III (`EDA`):** Análisis exploratorio de distribuciones, detección de valores atípicos mediante rango intercuartílico (`IQR`), probabilidades condicionales $P(Y \mid X)$ y matriz de correlación de Pearson ($r$).
  - **Fase IV (`scikit-learn`):** División estratificada `train_test_split` (80% entrenamiento / 20% prueba), `Pipeline` con `ColumnTransformer` (`StandardScaler` + `OneHotEncoder`), modelos base (`LinearSVC` y `DecisionTreeClassifier`), optimización exhaustiva con `GridSearchCV` y validación cruzada de 5 particiones (`StratifiedKFold(n_splits=5)`).
- **`index.html`**: Observatorio web interactivo autónomo (HTML5 + CSS3 + SVG + Leaflet) con filtros por píldoras, gráfico de líneas/área de ancho completo, gráfico de anillo, *treemap*, mapa de calor temporal (día vs. hora), mapa geográfico de puntos críticos y tabla dinámica con encabezados fijos (*sticky headers*).
- **`datos_accidentes_yb9r_2dsi.csv`**: Dataset completo limpio y normalizado (28,864 registros).
- **`datos_accidentes_filtrado_avanzado.csv`**: Subconjunto de siniestros críticos recientes obtenido mediante la condición booleana anidada (9,753 registros).
- **`requirements.txt`**: Dependencias del entorno de Python.

## Ejecución

1. **Visor interactivo:** Abrir directamente el archivo `index.html` en cualquier navegador web.
2. **Pipeline en Python:**
   ```bash
   python conexion_api.py
   ```
