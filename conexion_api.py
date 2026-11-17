"""
================================================================================
PROYECTO FINAL: CIENCIA DE DATOS Y MINERÍA DE DATOS CON PYTHON
Dataset: Accidentalidad Vial - Datos Abiertos Colombia (ID: yb9r-2dsi)
Endpoint: https://www.datos.gov.co/api/v3/views/yb9r-2dsi/query.json

================================================================================
"""

import json
import urllib.parse
import urllib.request
from typing import Any

import numpy as np
import pandas as pd

# ==============================================================================
# MÓDULO 0: CONFIGURACIÓN GLOBAL Y VARIABLES DINÁMICAS
# ==============================================================================
# Identificador único de la vista en el portal de Datos Abiertos Colombia (Socrata)
DATASET_ID: str = "yb9r-2dsi"

# Endpoints oficiales: API v3 de consulta directa y API SODA para paginación secuencial
API_V3_URL: str = f"https://www.datos.gov.co/api/v3/views/{DATASET_ID}/query.json"
API_SODA_URL: str = f"https://www.datos.gov.co/resource/{DATASET_ID}.json"

# Variables dinámicas que controlan los bucles de extracción y categorización
TAMANO_PAGINA: int = 5000
ANIOS_OBJETIVO: list[str] = [
    "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026"
]
CATEGORIAS_GRAVEDAD: list[str] = ["Solo daños", "Con heridos", "Con muertos"]


# ==============================================================================
# MÓDULO 1 (FASE I): EXTRACCIÓN SECUENCIAL Y CONSOLIDACIÓN MASIVA
# ==============================================================================
def construir_url_paginada(base_url: str, limite: int, offset: int) -> str:
    """
    Construye dinámicamente la URL de consulta inyectando los parámetros
    de límite ($limit), desplazamiento ($offset) y ordenamiento ($order).
    """
    parametros = urllib.parse.urlencode({
        "$limit": limite,
        "$offset": offset,
        "$order": ":id",
    })
    return f"{base_url}?{parametros}"


def descargar_fragmento_api(url: str, timeout: int = 60) -> list[dict[str, Any]]:
    """
    Ejecuta una petición HTTP GET hacia el endpoint especificado, valida el
    código de respuesta y decodifica el cuerpo JSON en una lista de diccionarios.
    """
    peticion = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (DataSciencePipeline/1.0)",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(peticion, timeout=timeout) as respuesta:
        if respuesta.status != 200:
            raise ConnectionError(f"Error HTTP inesperado: {respuesta.status}")
        contenido_texto = respuesta.read().decode("utf-8")
        return json.loads(contenido_texto)


def consolidar_lote_en_estructuras(
    lote_pagina: list[dict[str, Any]],
    lista_estructurada: list[dict[str, Any]],
    diccionario_consolidado: dict[str, Any],
) -> None:
    """
    Itera mediante un bucle `for` sobre cada registro del lote descargado para
    acumularlo de forma masiva tanto en la lista estructurada como en las
    agrupaciones jerárquicas del diccionario consolidado (por año, gravedad y clase).
    """
    for item in lote_pagina:
        # 1. Agregar registro a la lista estructurada global
        lista_estructurada.append(item)

        # 2. Extraer llaves dinámicas de clasificación para el diccionario estructurado
        anio_item = str(item.get("a_o_accidente", "Sin año")).strip()
        gravedad_item = str(item.get("gravedad_accidente", "Sin dato")).strip()
        clase_item = str(item.get("clase_accidente", "Sin dato")).strip()

        # 3. Insertar secuencialmente en cada partición del diccionario
        diccionario_consolidado["por_anio"].setdefault(anio_item, []).append(item)
        diccionario_consolidado["por_categoria_gravedad"].setdefault(gravedad_item, []).append(item)
        diccionario_consolidado["por_clase_accidente"].setdefault(clase_item, []).append(item)


def fase_1_extraccion_secuencial(
    tamano_lote: int = TAMANO_PAGINA,
    anios: list[str] = ANIOS_OBJETIVO,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[pd.DataFrame]]:
    """
    Orquesta la Fase I: Utiliza variables dinámicas y un bucle `while` para
    recorrer secuencialmente las páginas de la API hasta agotar los registros,
    apoyándose en un bucle `for` interno para consolidar la data en estructuras
    masivas y recolectar los fragmentos DataFrame para la Fase II.
    """
    print("=" * 78)
    print("FASE I: EXTRACCIÓN MASIVA SECUENCIAL (VARIABLES DINÁMICAS + BUCLE WHILE/FOR)")
    print("=" * 78)

    # Inicialización de estructuras de consolidación masiva
    lista_estructurada: list[dict[str, Any]] = []
    fragmentos_df: list[pd.DataFrame] = []
    diccionario_consolidado: dict[str, Any] = {
        "metadata_extraccion": {
            "dataset_id": DATASET_ID,
            "endpoint_v3": API_V3_URL,
            "endpoint_paginado": API_SODA_URL,
            "tamano_pagina": tamano_lote,
            "iteraciones_paginas": [],
            "total_registros_crudos": 0,
        },
        "por_anio": {anio: [] for anio in anios},
        "por_categoria_gravedad": {cat: [] for cat in CATEGORIAS_GRAVEDAD},
        "por_clase_accidente": {},
    }

    # Variables dinámicas de control para el bucle `while` de paginación
    pagina_actual: int = 1
    offset_dinamico: int = 0
    continuar_paginacion: bool = True

    while continuar_paginacion:
        url_dinamica = construir_url_paginada(API_SODA_URL, tamano_lote, offset_dinamico)
        print(f"[Página {pagina_actual}] Consultando offset={offset_dinamico}, limit={tamano_lote}...")

        # Descargar el fragmento correspondiente a la página actual
        lote_pagina = descargar_fragmento_api(url_dinamica)
        cantidad_recibida = len(lote_pagina)
        print(f"   -> Fragmento {pagina_actual} recibido: {cantidad_recibida:,} registros.")

        # Almacenar el fragmento como DataFrame independiente para concatenar en Fase II
        fragmentos_df.append(pd.DataFrame(lote_pagina))

        # Consolidar registros en la lista y diccionario mediante bucle `for`
        consolidar_lote_en_estructuras(lote_pagina, lista_estructurada, diccionario_consolidado)

        # Guardar metadatos de trazabilidad de la iteración actual
        diccionario_consolidado["metadata_extraccion"]["iteraciones_paginas"].append({
            "pagina": pagina_actual,
            "offset": offset_dinamico,
            "registros_obtenidos": cantidad_recibida,
        })

        # Evaluar condición de parada: si el lote tiene menos filas que el límite, es la última página
        if cantidad_recibida < tamano_lote:
            continuar_paginacion = False
        else:
            offset_dinamico += tamano_lote
            pagina_actual += 1

    diccionario_consolidado["metadata_extraccion"]["total_registros_crudos"] = len(lista_estructurada)
    return lista_estructurada, diccionario_consolidado, fragmentos_df


# ==============================================================================
# MÓDULO 2 (FASE II): MANIPULACIÓN Y LIMPIEZA AVANZADA (PANDAS & NUMPY)
# ==============================================================================
def concatenar_fragmentos(fragmentos_df: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Une eficientemente todos los fragmentos extraídos usando `pd.concat()`
    y descarta columnas internas de metadatos del motor Socrata (prefijo ':').
    """
    print(f"\n[1] Concatenando {len(fragmentos_df)} fragmentos de datos con pd.concat()...")
    df_crudo = pd.concat(fragmentos_df, ignore_index=True)
    print(f"    -> Dimensiones tras pd.concat(): {df_crudo.shape[0]:,} filas x {df_crudo.shape[1]} columnas")

    columnas_metadatos = [col for col in df_crudo.columns if col.startswith(":")]
    if columnas_metadatos:
        df_crudo = df_crudo.drop(columns=columnas_metadatos)

    return df_crudo


def tratar_nulos_y_duplicados(df_entrada: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Identifica y trata explícitamente los valores faltantes (NaN) y los registros
    duplicados (tanto exactos como por llave compuesta de negocio).
    """
    print("\n[2] Auditoría y tratamiento explícito de valores faltantes (NaN) y duplicados:")
    df = df_entrada.copy()

    # Transformar cadenas vacías o compuestas solo por espacios en np.nan reales
    df = df.replace(r"^\s*$", np.nan, regex=True)

    # Auditar cantidad y porcentaje de valores NaN por cada columna antes de limpiar
    conteo_nan_antes = df.isna().sum()
    print("    -> Valores faltantes (NaN) detectados por columna ANTES de limpieza:")
    for nombre_col, total_nulos in conteo_nan_antes.items():
        porcentaje_nulos = (total_nulos / len(df)) * 100
        print(f"       * {nombre_col:<34}: {total_nulos:>6,} NaN ({porcentaje_nulos:5.2f}%)")

    # Definir llave compuesta que identifica unívocamente un evento de tránsito
    llave_evento = [
        "fecha_accidente",
        "hora_accidente",
        "sitio_exacto_accidente",
        "clase_accidente",
        "gravedad_accidente",
    ]
    duplicados_exactos = int(df.duplicated().sum())
    duplicados_llave = int(df.duplicated(subset=llave_evento).sum())
    print(f"\n    -> Registros duplicados exactos detectados          : {duplicados_exactos:,}")
    print(f"    -> Registros duplicados por llave de evento (subset): {duplicados_llave:,}")

    # Eliminar duplicados conservando la primera ocurrencia verificada
    filas_antes = len(df)
    df = df.drop_duplicates(subset=llave_evento, keep="first").reset_index(drop=True)
    duplicados_eliminados = filas_antes - len(df)
    print(f"    -> Duplicados eliminados con drop_duplicates(): {duplicados_eliminados:,} (Filas restantes: {len(df):,})")

    # Imputación contextual de valores faltantes (NaN):
    # En conteo de heridos y muertos, la ausencia de valor en el reporte equivale a 0 víctimas
    df["cant_heridos_en_sitio_accidente"] = df["cant_heridos_en_sitio_accidente"].fillna("0")
    df["cant_muertos_en_sitio_accidente"] = df["cant_muertos_en_sitio_accidente"].fillna("0")
    df["cantidad_accidentes"] = df["cantidad_accidentes"].fillna("1")

    # Imputación de atributos descriptivos y descarte de filas sin fecha ni año válido
    df["gravedad_accidente"] = df["gravedad_accidente"].fillna("Sin especificar")
    df["clase_accidente"] = df["clase_accidente"].fillna("Otro")
    df["sitio_exacto_accidente"] = df["sitio_exacto_accidente"].fillna("Dirección no registrada")
    df = df.dropna(subset=["fecha_accidente", "a_o_accidente"]).reset_index(drop=True)

    nan_restantes = int(df.isna().sum().sum())
    print(f"    -> Total de valores NaN en el DataFrame DESPUÉS del tratamiento: {nan_restantes}")

    metricas_auditoria = {
        "duplicados_exactos_detectados": duplicados_exactos,
        "duplicados_eliminados": duplicados_eliminados,
        "nan_heridos_imputados": int(conteo_nan_antes.get("cant_heridos_en_sitio_accidente", 0)),
        "nan_muertos_imputados": int(conteo_nan_antes.get("cant_muertos_en_sitio_accidente", 0)),
        "nan_restantes_post_limpieza": nan_restantes,
    }
    return df, metricas_auditoria


def castear_tipos_y_calcular_metricas(df_entrada: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza la conversión y casteo explícito de columnas tipo texto (str) hacia
    tipos numéricos de NumPy (`int64`, `float64`) y ejecuta operaciones matemáticas
    vectorizadas para construir variables derivadas de severidad.
    """
    print("\n[3] Conversión y casteo explícito de tipos texto (str) a tipos numéricos (NumPy/Pandas):")
    df = df_entrada.copy()

    print("    -> Tipos de datos ANTES del casteo:")
    print(df.dtypes.to_string())

    # Conversión mediante pd.to_numeric() y casteo explícito con .astype() de NumPy
    df["a_o_accidente"] = (
        pd.to_numeric(df["a_o_accidente"], errors="coerce").fillna(0).astype(np.int64)
    )
    df["cant_heridos_en_sitio_accidente"] = (
        pd.to_numeric(df["cant_heridos_en_sitio_accidente"], errors="coerce")
        .fillna(0.0)
        .astype(np.float64)
    )
    df["cant_muertos_en_sitio_accidente"] = (
        pd.to_numeric(df["cant_muertos_en_sitio_accidente"], errors="coerce")
        .fillna(0.0)
        .astype(np.float64)
    )
    df["cantidad_accidentes"] = (
        pd.to_numeric(df["cantidad_accidentes"], errors="coerce").fillna(1.0).astype(np.int64)
    )

    # Normalizar formato de fecha ISO (YYYY-MM-DD)
    df["fecha_accidente"] = df["fecha_accidente"].astype(str).str.split("T").str[0]

    # Traducción de meses y días de la semana del inglés al español
    mapa_meses_es = {
        "January": "Enero", "February": "Febrero", "March": "Marzo",
        "April": "Abril", "May": "Mayo", "June": "Junio",
        "July": "Julio", "August": "Agosto", "September": "Septiembre",
        "October": "Octubre", "November": "Noviembre", "December": "Diciembre",
    }
    mapa_dias_es = {
        "Mon": "Lunes", "Tue": "Martes", "Wed": "Miércoles",
        "Thu": "Jueves", "Fri": "Viernes", "Sat": "Sábado", "Sun": "Domingo",
        "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
        "Thursday": "Jueves", "Friday": "Viernes", "Saturday": "Sábado", "Sunday": "Domingo",
    }
    df["mes_accidente"] = df["mes_accidente"].astype(str).str.strip().replace(mapa_meses_es)
    df["dia_accidente"] = df["dia_accidente"].astype(str).str.strip().replace(mapa_dias_es)

    # Procesamiento matemático vectorizado con NumPy:
    # 1. Suma vectorizada de heridos + muertos para obtener el total de víctimas en sitio
    df["total_victimas"] = np.add(
        df["cant_heridos_en_sitio_accidente"].to_numpy(),
        df["cant_muertos_en_sitio_accidente"].to_numpy(),
    ).astype(np.float64)

    # 2. Cálculo ponderado del Índice de Severidad Vial (1 pto base + 3 por herido + 10 por fallecido)
    df["indice_severidad"] = (
        df["cantidad_accidentes"].to_numpy() * 1.0
        + df["cant_heridos_en_sitio_accidente"].to_numpy() * 3.0
        + df["cant_muertos_en_sitio_accidente"].to_numpy() * 10.0
    ).astype(np.float64)

    print("\n    -> Tipos de datos DESPUÉS del casteo explícito:")
    print(df.dtypes.to_string())
    return df


def aplicar_filtrado_avanzado_anidado(df_limpio: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica un filtrado avanzado utilizando condiciones múltiples anidadas mediante
    los operadores lógicos `&` (AND) y `|` (OR), agrupadas rigurosamente con
    paréntesis para garantizar la precedencia correcta en Python/Pandas.
    """
    print("\n[4] Ejecutando Filtrado Avanzado con condiciones múltiples anidadas (& y |):")

    # Lógica de la condición anidada:
    #   (Bloque Temporal: Años entre 2020 y 2026)
    #   & (Bloque de Severidad: (Gravedad con heridos o muertos) & (al menos 1 víctima registrada))
    #   & (Bloque de Modalidad: Choque | Atropello | Volcamiento)
    mascara_logica_anidada = (
        (
            (df_limpio["a_o_accidente"] >= 2020)
            & (df_limpio["a_o_accidente"] <= 2026)
        )
        & (
            (
                (df_limpio["gravedad_accidente"] == "Con heridos")
                | (df_limpio["gravedad_accidente"] == "Con muertos")
            )
            & (
                (df_limpio["cant_heridos_en_sitio_accidente"] >= 1.0)
                | (df_limpio["cant_muertos_en_sitio_accidente"] >= 1.0)
            )
        )
        & (
            (df_limpio["clase_accidente"] == "Choque")
            | (df_limpio["clase_accidente"] == "Atropello")
            | (df_limpio["clase_accidente"] == "Volcamiento")
        )
    )

    df_filtrado = df_limpio.loc[mascara_logica_anidada].copy().reset_index(drop=True)

    porcentaje = (len(df_filtrado) / len(df_limpio)) * 100
    print(f"    -> Registros totales en df_limpio           : {len(df_limpio):,}")
    print(f"    -> Registros que cumplen condición anidada  : {len(df_filtrado):,} ({porcentaje:.2f}%)")
    print(f"    -> Promedio de víctimas en subconjunto crítico (np.mean): {np.mean(df_filtrado['total_victimas']):.2f}")
    print(f"    -> Índice de severidad máximo (np.max)                  : {np.max(df_filtrado['indice_severidad']):.1f}")

    return df_filtrado


def fase_2_limpieza_y_manipulacion_avanzada(
    fragmentos_df: list[pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """
    Orquesta todos los submódulos de la Fase II: concatenación con `pd.concat()`,
    limpieza de nulos/duplicados, casteo numérico explícito y filtrado booleano anidado.
    """
    print("\n" + "=" * 78)
    print("FASE II: MANIPULACIÓN Y LIMPIEZA AVANZADA (PANDAS & NUMPY)")
    print("=" * 78)

    df_crudo = concatenar_fragmentos(fragmentos_df)
    df_sin_nulos, metricas_nulos = tratar_nulos_y_duplicados(df_crudo)
    df_limpio = castear_tipos_y_calcular_metricas(df_sin_nulos)
    df_filtrado_avanzado = aplicar_filtrado_avanzado_anidado(df_limpio)

    reporte_limpieza = {
        "fragmentos_concatenados": len(fragmentos_df),
        "filas_crudas_concatenadas": int(df_crudo.shape[0]),
        **metricas_nulos,
        "filas_limpias_finales": int(len(df_limpio)),
        "filas_filtro_avanzado_anidado": int(len(df_filtrado_avanzado)),
    }
    return df_limpio, df_filtrado_avanzado, reporte_limpieza


# ==============================================================================
# MÓDULO 3 (FASE III): ANÁLISIS EXPLORATORIO Y VISUALIZACIÓN ESTRUCTURADA (EDA)
# ==============================================================================
def extraer_hora_militar_vectorizada(serie_hora: pd.Series) -> np.ndarray:
    """
    Convierte cadenas de hora en formato 12h ('01:30:00:pm') a enteros en formato
    24 horas (0 a 23) utilizando operaciones vectorizadas de Pandas y NumPy.
    """
    texto_hora = serie_hora.astype(str).str.lower().str.strip()
    hora_base = pd.to_numeric(texto_hora.str.split(":").str[0], errors="coerce").fillna(12).astype(np.int64)
    es_pm = texto_hora.str.contains("pm", na=False)
    es_am = texto_hora.str.contains("am", na=False)

    return np.where(
        es_pm & (hora_base < 12),
        hora_base + 12,
        np.where(es_am & (hora_base == 12), 0, hora_base),
    )


def fase_3_analisis_exploratorio_eda(df_limpio: pd.DataFrame) -> dict[str, Any]:
    """
    Orquesta la Fase III: Analiza distribuciones, detecta valores atípicos mediante
    el rango intercuartílico (IQR), construye tablas de probabilidad condicional
    P(Y | X) y calcula la matriz de correlación de Pearson antes del modelado.
    """
    print("\n" + "=" * 78)
    print("FASE III: ANÁLISIS EXPLORATORIO DE DATOS (EDA: VARIABLES X vs OBJETIVO Y)")
    print("=" * 78)

    df_eda = df_limpio.copy()

    # 1. Codificación de la Variable Objetivo (Y) y Variables Independientes (X)
    df_eda["Y_tiene_victimas"] = np.where(
        (df_eda["gravedad_accidente"] != "Solo daños") | (df_eda["total_victimas"] >= 1.0),
        1,
        0,
    ).astype(np.int64)

    df_eda["X1_clase_vulnerable"] = np.where(
        df_eda["clase_accidente"].isin(["Atropello", "Caida Ocupante", "Volcamiento"]),
        1,
        0,
    ).astype(np.int64)

    df_eda["hora_24"] = extraer_hora_militar_vectorizada(df_eda["hora_accidente"])
    df_eda["X2_franja_nocturna"] = np.where(
        (df_eda["hora_24"] >= 19) | (df_eda["hora_24"] <= 5), 1, 0
    ).astype(np.int64)

    df_eda["X3_fin_de_semana"] = np.where(
        df_eda["dia_accidente"].isin(["Sat", "Sun", "Sábado", "Domingo"]), 1, 0
    ).astype(np.int64)

    # 2. Distribución de la Variable Objetivo y Detección de Outliers por regla de Tukey (IQR)
    dist_gravedad = df_eda["gravedad_accidente"].value_counts(normalize=True) * 100
    q25, mediana, q75 = np.percentile(df_eda["total_victimas"], [25, 50, 75])
    iqr = q75 - q25
    umbral_outlier = q75 + (1.5 * iqr)
    num_outliers = int((df_eda["total_victimas"] > umbral_outlier).sum())
    max_victimas = float(df_eda["total_victimas"].max())

    print("\n[1] Distribución de la Variable Objetivo (Y) y Detección de Outliers (IQR):")
    for categoria, porcentaje in dist_gravedad.items():
        conteo_cat = int((df_eda["gravedad_accidente"] == categoria).sum())
        print(f"    * {categoria:<18}: {porcentaje:6.2f}% ({conteo_cat:,} casos)")
    print(f"    * Cuartiles de Total Víctimas: Q1={q25:.1f}, Mediana={mediana:.1f}, Q3={q75:.1f}, IQR={iqr:.1f}")
    print(f"    * Umbral superior IQR (Q3 + 1.5*IQR): {umbral_outlier:.2f} víctimas")
    print(f"    * Outliers detectados (>= 3 víctimas): {num_outliers:,} eventos ({(num_outliers/len(df_eda))*100:.2f}%), Máximo={max_victimas:.0f}")

    # 3. Relación Bivariada: Probabilidad Condicional P(Gravedad Y | Clase de Accidente X1)
    print("\n[2] Tabla de Probabilidad Condicional P(Gravedad Y | Clase de Accidente X1) [%]:")
    tabla_cruzada_pct = pd.crosstab(
        df_eda["clase_accidente"],
        df_eda["gravedad_accidente"],
        normalize="index",
    ) * 100
    print(tabla_cruzada_pct.round(2).to_string())

    # 4. Matriz de Correlación de Pearson entre Predictoras (X) y Variables Objetivo (Y)
    cols_corr = [
        "X1_clase_vulnerable",
        "X2_franja_nocturna",
        "X3_fin_de_semana",
        "a_o_accidente",
        "Y_tiene_victimas",
        "total_victimas",
        "indice_severidad",
    ]
    matriz_corr = df_eda[cols_corr].corr(method="pearson")
    print("\n[3] Matriz de Correlación de Pearson (Variables X vs Variables Objetivo Y):")
    print(matriz_corr[["Y_tiene_victimas", "total_victimas", "indice_severidad"]].round(4).to_string())

    return {
        "q25": float(q25),
        "mediana": float(mediana),
        "q75": float(q75),
        "iqr": float(iqr),
        "umbral_outlier": float(umbral_outlier),
        "num_outliers": num_outliers,
        "max_victimas": max_victimas,
        "tabla_cruzada_pct": tabla_cruzada_pct.round(2).to_dict(),
        "correlaciones": matriz_corr.round(4).to_dict(),
    }


# ==============================================================================
# MÓDULO 4 (FASE IV): MACHINE LEARNING AVANZADO, GRIDSEARCHCV Y 5-FOLD CV
# ==============================================================================
def fase_4_machine_learning_y_optimizacion(df_limpio: pd.DataFrame) -> dict[str, Any]:
    """
    Orquesta la Fase IV utilizando `scikit-learn`:
      1. Partición estratificada en Train (80%) y Test (20%) ANTES de ajustar transformadores.
      2. Construcción de Pipeline con ColumnTransformer (StandardScaler + OneHotEncoder).
      3. Entrenamiento del modelo base (Support Vector Machine - LinearSVC y DecisionTree).
      4. Optimización exhaustiva de hiperparámetros con GridSearchCV y 5-Fold Cross-Validation.
      5. Documentación cuantitativa del incremento del score antes y después de optimizar.
    """
    from sklearn.compose import ColumnTransformer
    from sklearn.metrics import (
        accuracy_score,
        classification_report,
        confusion_matrix,
        f1_score,
        precision_score,
        recall_score,
    )
    from sklearn.model_selection import (
        GridSearchCV,
        StratifiedKFold,
        cross_val_score,
        train_test_split,
    )
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from sklearn.svm import LinearSVC
    from sklearn.tree import DecisionTreeClassifier

    print("\n" + "=" * 78)
    print("FASE IV: MACHINE LEARNING AVANZADO Y OPTIMIZACIÓN (SCIKIT-LEARN)")
    print("=" * 78)

    df_ml = df_limpio.copy()
    df_ml["hora_24"] = extraer_hora_militar_vectorizada(df_ml["hora_accidente"])
    df_ml["anio_cat"] = df_ml["a_o_accidente"].astype(str)

    # Definición de la variable objetivo binaria Y: 1 = Con víctimas, 0 = Solo daños
    df_ml["Y_tiene_victimas"] = np.where(
        (df_ml["gravedad_accidente"] != "Solo daños") | (df_ml["total_victimas"] >= 1.0),
        1,
        0,
    ).astype(np.int64)

    features_numericas = ["hora_24"]
    features_categoricas = ["anio_cat", "clase_accidente", "mes_accidente", "dia_accidente"]

    X = df_ml[features_numericas + features_categoricas]
    y = df_ml["Y_tiene_victimas"]

    # 1. División formal Train (80%) / Test (20%) estratificada antes del preprocesamiento
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )
    print(f"\n[1] División formal del dataset estructurado (Estratificada 80% / 20%):")
    print(f"    * Subconjunto de Entrenamiento (X_train): {X_train.shape[0]:,} registros (80.0%)")
    print(f"    * Subconjunto de Prueba        (X_test) : {X_test.shape[0]:,} registros (20.0%)")

    # Preprocesador de columnas integrado al Pipeline para prevenir fuga de datos (Data Leakage)
    preprocesador = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), features_numericas),
            ("cat", OneHotEncoder(handle_unknown="ignore"), features_categoricas),
        ]
    )

    # Configuración del esquema de Validación Cruzada Estratificada de 5 Folds
    esquema_cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    # 2. Modelo Predictivo Base: Support Vector Machine (LinearSVC sin optimizar)
    print("\n[2] Evaluando Modelo Base 1: Support Vector Machine (LinearSVC base, C=0.0002)...")
    pipeline_svm_base = Pipeline(
        steps=[
            ("preprocesamiento", preprocesador),
            ("clasificador", LinearSVC(C=0.0002, class_weight=None, dual="auto", random_state=42)),
        ]
    )
    scores_cv_svm_base = cross_val_score(
        pipeline_svm_base, X_train, y_train, cv=esquema_cv, scoring="accuracy"
    )
    pipeline_svm_base.fit(X_train, y_train)
    y_pred_svm_base = pipeline_svm_base.predict(X_test)
    acc_test_svm_base = float(accuracy_score(y_test, y_pred_svm_base))
    f1_test_svm_base = float(f1_score(y_test, y_pred_svm_base))

    print(f"    * Scores en los 5 Folds (CV Base): {np.round(scores_cv_svm_base, 4)}")
    print(f"    * Media CV (5 Folds) SVM Base    : {scores_cv_svm_base.mean()*100:.2f}% (±{scores_cv_svm_base.std()*100:.2f}%)")
    print(f"    * Accuracy en Test SVM Base      : {acc_test_svm_base*100:.2f}% | F1-Score: {f1_test_svm_base*100:.2f}%")

    # 3. Optimización de Hiperparámetros con GridSearchCV (5 Folds) sobre Support Vector Machine
    print("\n[3] Ejecutando GridSearchCV (5 Folds) sobre Support Vector Machine (LinearSVC)...")
    grilla_parametros_svm = {
        "clasificador__C": [0.0002, 0.001, 0.01, 0.1, 1.0, 5.0],
        "clasificador__class_weight": [None, "balanced"],
    }
    grid_search_svm = GridSearchCV(
        estimator=Pipeline(
            steps=[
                ("preprocesamiento", preprocesador),
                ("clasificador", LinearSVC(dual="auto", max_iter=10000, random_state=42)),
            ]
        ),
        param_grid=grilla_parametros_svm,
        cv=esquema_cv,
        scoring="accuracy",
        n_jobs=1,
    )
    grid_search_svm.fit(X_train, y_train)

    mejor_svm = grid_search_svm.best_estimator_
    scores_cv_svm_opt = cross_val_score(
        mejor_svm, X_train, y_train, cv=esquema_cv, scoring="accuracy"
    )
    y_pred_svm_opt = mejor_svm.predict(X_test)

    acc_test_svm_opt = float(accuracy_score(y_test, y_pred_svm_opt))
    f1_test_svm_opt = float(f1_score(y_test, y_pred_svm_opt))
    prec_test_svm_opt = float(precision_score(y_test, y_pred_svm_opt))
    rec_test_svm_opt = float(recall_score(y_test, y_pred_svm_opt))
    cm_svm_opt = confusion_matrix(y_test, y_pred_svm_opt)

    print(f"    * Mejores Hiperparámetros hallados (best_params_): {grid_search_svm.best_params_}")
    print(f"    * Scores en los 5 Folds (CV Optimizado): {np.round(scores_cv_svm_opt, 4)}")
    print(f"    * Media CV (5 Folds) SVM Optimizado    : {scores_cv_svm_opt.mean()*100:.2f}% (±{scores_cv_svm_opt.std()*100:.2f}%)")
    print(f"    * Accuracy en Test SVM Optimizado      : {acc_test_svm_opt*100:.2f}% | F1-Score: {f1_test_svm_opt*100:.2f}%")
    print(f"    * Precisión (Precision): {prec_test_svm_opt*100:.2f}% | Sensibilidad (Recall): {rec_test_svm_opt*100:.2f}%")

    # 4. Modelo Comparativo No Lineal: DecisionTreeClassifier Base vs GridSearchCV (5 Folds)
    print("\n[4] Evaluando Modelo 2 (No Lineal): DecisionTreeClassifier Base vs GridSearchCV (5 Folds)...")
    pipeline_dt_base = Pipeline(
        steps=[
            ("preprocesamiento", preprocesador),
            ("clasificador", DecisionTreeClassifier(max_depth=2, random_state=42)),
        ]
    )
    scores_cv_dt_base = cross_val_score(
        pipeline_dt_base, X_train, y_train, cv=esquema_cv, scoring="accuracy"
    )
    pipeline_dt_base.fit(X_train, y_train)
    acc_test_dt_base = float(accuracy_score(y_test, pipeline_dt_base.predict(X_test)))

    grilla_parametros_dt = {
        "clasificador__criterion": ["gini", "entropy"],
        "clasificador__max_depth": [3, 5, 7, 9, 12],
        "clasificador__min_samples_split": [10, 50, 100],
    }
    grid_search_dt = GridSearchCV(
        estimator=Pipeline(
            steps=[
                ("preprocesamiento", preprocesador),
                ("clasificador", DecisionTreeClassifier(random_state=42)),
            ]
        ),
        param_grid=grilla_parametros_dt,
        cv=esquema_cv,
        scoring="accuracy",
        n_jobs=1,
    )
    grid_search_dt.fit(X_train, y_train)
    acc_test_dt_opt = float(accuracy_score(y_test, grid_search_dt.predict(X_test)))

    # 5. Documentación del incremento de precisión antes y después de la optimización
    inc_cv_svm = (scores_cv_svm_opt.mean() - scores_cv_svm_base.mean()) * 100
    inc_test_svm = (acc_test_svm_opt - acc_test_svm_base) * 100
    inc_f1_svm = (f1_test_svm_opt - f1_test_svm_base) * 100
    inc_cv_dt = (grid_search_dt.best_score_ - scores_cv_dt_base.mean()) * 100
    inc_test_dt = (acc_test_dt_opt - acc_test_dt_base) * 100

    print("\n" + "-" * 78)
    print("RESUMEN COMPARATIVO DE OPTIMIZACIÓN CON GRIDSEARCHCV (5-FOLD CROSS-VALIDATION)")
    print("-" * 78)
    print(f"  * SVM (LinearSVC) CV 5-Folds  : Antes = {scores_cv_svm_base.mean()*100:.2f}% -> Después = {scores_cv_svm_opt.mean()*100:.2f}% (Incremento: +{inc_cv_svm:.2f}%)")
    print(f"  * SVM (LinearSVC) Test Acc    : Antes = {acc_test_svm_base*100:.2f}% -> Después = {acc_test_svm_opt*100:.2f}% (Incremento: +{inc_test_svm:.2f}%)")
    print(f"  * SVM (LinearSVC) Test F1     : Antes = {f1_test_svm_base*100:.2f}% -> Después = {f1_test_svm_opt*100:.2f}% (Incremento: +{inc_f1_svm:.2f}%)")
    print(f"  * Árbol Decisión  CV 5-Folds  : Antes = {scores_cv_dt_base.mean()*100:.2f}% -> Después = {grid_search_dt.best_score_*100:.2f}% (Incremento: +{inc_cv_dt:.2f}%)")
    print(f"  * Árbol Decisión  Test Acc    : Antes = {acc_test_dt_base*100:.2f}% -> Después = {acc_test_dt_opt*100:.2f}% (Incremento: +{inc_test_dt:.2f}%)")
    print("\nMatriz de Confusión en Test (SVM Optimizado):")
    print(cm_svm_opt)
    print("\nReporte de Clasificación en Test (SVM Optimizado):")
    print(classification_report(y_test, y_pred_svm_opt, target_names=["Solo daños (0)", "Con víctimas (1)"]))

    return {
        "train_size": int(len(X_train)),
        "test_size": int(len(X_test)),
        "svm_base_cv_mean": float(scores_cv_svm_base.mean()),
        "svm_opt_cv_mean": float(scores_cv_svm_opt.mean()),
        "svm_opt_test_acc": acc_test_svm_opt,
        "svm_opt_test_f1": f1_test_svm_opt,
        "dt_base_cv_mean": float(scores_cv_dt_base.mean()),
        "dt_opt_cv_mean": float(grid_search_dt.best_score_),
        "dt_opt_test_acc": acc_test_dt_opt,
        "confusion_matrix": cm_svm_opt.tolist(),
    }


# ==============================================================================
# MÓDULO 5 (MAIN): ORQUESTADOR GENERAL DEL PIPELINE Y EXPORTACIÓN
# ==============================================================================
def ejecutar_pipeline_completo() -> None:
    """
    Función principal que coordina secuencialmente las 4 fases del proyecto:
      - Fase I  : Extracción paginada y consolidación masiva.
      - Fase II : Limpieza avanzada, casteo numérico y filtrado booleano anidado.
      - Fase III: Análisis Exploratorio de Datos (EDA) y correlaciones.
      - Fase IV : Entrenamiento y optimización de modelos con GridSearchCV y 5-Fold CV.
    """
    # 1. Fase I: Extracción Secuencial
    _, _, lista_fragmentos = fase_1_extraccion_secuencial()

    # 2. Fase II: Manipulación y Limpieza Avanzada
    df_limpio_final, df_criticos_filtrado, _ = fase_2_limpieza_y_manipulacion_avanzada(
        lista_fragmentos
    )

    # 3. Fase III: Análisis Exploratorio (EDA)
    fase_3_analisis_exploratorio_eda(df_limpio_final)

    # 4. Fase IV: Machine Learning y Optimización
    fase_4_machine_learning_y_optimizacion(df_limpio_final)

    # 5. Exportación de datasets procesados a disco
    output_csv_limpio = "datos_accidentes_yb9r_2dsi.csv"
    output_csv_filtrado = "datos_accidentes_filtrado_avanzado.csv"

    df_limpio_final.to_csv(output_csv_limpio, index=False, encoding="utf-8-sig")
    df_criticos_filtrado.to_csv(output_csv_filtrado, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 78)
    print("PIPELINE FINALIZADO EXITOSAMENTE — ARCHIVOS GENERADOS:")
    print(f"  1. Dataset limpio completo : {output_csv_limpio} ({len(df_limpio_final):,} filas)")
    print(f"  2. Subconjunto filtro (&,|): {output_csv_filtrado} ({len(df_criticos_filtrado):,} filas)")
    print("=" * 78)


if __name__ == "__main__":
    ejecutar_pipeline_completo()
