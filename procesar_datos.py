import os
import io
import zipfile
import logging
import requests
import pandas as pd
from datetime import datetime
import pytz

# ========================
# CONFIGURACIÓN DE LOGGING
# ========================
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# ========================
# DICCIONARIO DE PALABRAS CLAVE
# ========================
KEYWORDS = {
    "Arma de fuego": [
        "arma de fuego", "arma de fuefo", "arma de fueego", "arna de fuego", "proyectíl",
        "proyectil", "proyectiles", "proyectikl", "poyectil", "paf", "bala", "porbala",
        "armade", "perdigon", "perdigones", "disparo", "disparado", "percutado", "p.a.f.", "pafen",
        "p.a.f", "p. a. f.", "perforo contusas", "perforo contusa", "perforo-contusa",
        "perforo-contusas", "perforo - contusas", "perforo - contusa", "perforocontusas",
        "perforocontusa", "perfocontusas", "perforo-contuso", "perforo contuso", "peroforo-contusas",
        "perforo - contuso", "perforo -contusa", "ferforo - contusas", "perforocontuss", "contuso perforante", "orificio de entrada",
        "heridas multiples por fdf", "herida perforante", "heridas perforantes", "lesion perforante",
        "curso perforante", "curso perforantes",
        "curso penetrante"         
    ],
    "Arma blanca": [
        "arma blanca", "arma  blanca", "punzocortante", "punzocortarte", "punzo cortante", "punzo-cortante", "punzocortantes",
        "punzo - cortante", "punzo - cortantes", "punzo-cortantes", "punza - cortante", "lesion cortante", "lesión cortante",
        "punzo-cortantes", "punzopenetrante", "punzo-penetrante", "punzo - penetrante", "punzo penetrante", "punza cortante",
        "punzocortopenetrante", "punzocortoperforante", "punzo-corto-penetrante", "punzo corto penetrante", "ponzo cortantes",
        "punzo cortante penetrante", "punzo cortante penetrante", "punzocortante penetrante", "puncortantes", "punzocorto penetrante",
        "punzo-corto penetrante", "corto-punzo-penetrante", "punzo- penetrante", "corto punzante", "cortantes y penetrantes", "punzo-corzo-penetrante",
        "corto punzantes", "contusocortante", "contusocortantes", "contuso-cortante", "contusas cortantes", "corto penetrante", "corto penetrantes",
        "cortopenetrante", "cortopenetrantes", "corto-penetrante", "contuso-cortantes", "contuso cortante", "objeto cortante", "degüello", "deguello", "degúello",
        "degullo", "degollamiento", "herida cortante", "decapitación", "punzante", "punta - filo - hoja", "decapitacion",
        "punta y-o filo", "punta y filo", "punta yo filo", "botella rota", "seccion de traquea y vasos de cuello", "seccion de laringe y grandes vasos",
        "sección total de laringe y de vasos sanguineos", "seccionamiento de paquete vasculo nervioso cevical", "herdas cortantes",
        "heridas cortantes", "cuchillo", "filo y peso", "objeto con uso cortante", "traumatismos cortantes", "herida transfixiante en cuello"  
    ],    
    "Asfixia": [
        "estrangulación", "estrangulamiento", "estrangulacion", "asfixia", "asfiixia", "extrangulamiento",
        "asfixias", "ahorcamiento", "ahorcadura", "sofocacion", "ahogamiento"
    ]
}


# ========================
# FUNCIONES
# ========================
def download_csv(url: str, headers: dict):
    logging.info("📥 Descargando datos desde SINADEF...")
    try:
        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status()

        zip_buffer = io.BytesIO(response.content)
        with zipfile.ZipFile(zip_buffer) as z:
            nombres = z.namelist()
            logging.info(f"📦 Archivos en ZIP: {nombres}")

            csv_nombre = [n for n in nombres if n.endswith('.csv')][0]
            with z.open(csv_nombre) as csv_file:
                df = pd.read_csv(csv_file, sep=",", encoding="utf-8-sig", on_bad_lines="skip")

        logging.info(f"✅ Archivo descargado correctamente. Filas: {len(df)}")
        return df

    except Exception as e:
        logging.error(f"❌ Error al descargar o leer el archivo: {e}")
        return None  # 👈 CLAVE

def filter_necropsia_data(data: pd.DataFrame):
    data['MUERTE_VIOLENTA'] = data['MUERTE_VIOLENTA'].str.strip()
    data['NECROPSIA'] = data['NECROPSIA'].str.strip()

    filtered = data[
        (data['MUERTE_VIOLENTA'] == "HOMICIDIO") &
        (data['NECROPSIA'].str.contains("SI SE REALIZ", na=False)) &
        (data['ANIO'] >= 2017)
    ].copy()

    logging.info(f"✅ Filtrado completado: {filtered.shape[0]} registros")
    return filtered

def clasificar_causa(texto: str):
    texto = str(texto).lower()

    for categoria, palabras in KEYWORDS.items():
        if any(p in texto for p in palabras):
            return categoria

    return "Otra causa"

def clasificar_causa_row(row: pd.Series):
    columnas = ['DEBIDO_CAUSA_A', 'DEBIDO_CAUSA_B', 'DEBIDO_CAUSA_C',
                'DEBIDO_CAUSA_D', 'DEBIDO_CAUSA_E', 'DEBIDO_CAUSA_F']

    texto = " ".join(str(row[col]) for col in columnas if pd.notnull(row[col]))
    return clasificar_causa(texto)

def clasificar_edad(row: pd.Series):
    try:
        edad = float(row.get('EDAD', None))
    except:
        return "Sin registro"

    tiempo = str(row.get('TIEMPO_EDAD', "")).upper()

    if tiempo in ["MESES", "MINUTOS", "DIAS"]:
        return "Niño"
    elif tiempo == "AÑOS":
        if edad <= 11: return "Niño"
        elif edad <= 17: return "Adolescente"
        elif edad <= 29: return "Joven"
        elif edad <= 59: return "Adulto"
        else: return "Adulto mayor"

    return "Sin registro"

# ========================
# FLUJO PRINCIPAL
# ========================
def main():
    logging.info("🚀 Iniciando procesamiento...")

    url = "https://files.minsa.gob.pe/s/a6Hmynsenb7Px2y/download"
    headers = {"User-Agent": "Mozilla/5.0"}

    # 1. Descargar
    df = download_csv(url, headers)

    if df is None:
        logging.error("❌ No se pudo descargar el CSV. Se mantiene versión anterior.")
        return 1  # 👈 activa reintento

    # 2. Filtrar
    df_filtrado = filter_necropsia_data(df)

    if df_filtrado.empty:
        logging.warning("⚠️ Dataset vacío. No se actualizará el CSV.")
        return 1  # 👈 reintento

    # 3. Procesar
    df_filtrado["Grupo_Causa"] = df_filtrado.apply(clasificar_causa_row, axis=1)
    df_filtrado["EDADES"] = df_filtrado.apply(clasificar_edad, axis=1)

    df_filtrado["SEXO"] = df_filtrado["SEXO"].str.upper().str.strip()
    df_filtrado["SEXO"] = df_filtrado["SEXO"].replace({
        "FEMENINO": "Mujer",
        "MASCULINO": "Hombre"
    })

    # 4. Fecha Perú
    peru_tz = pytz.timezone('America/Lima')
    now = datetime.now(peru_tz)

    df_filtrado["FECHA_DESCARGA"] = now.strftime("%Y-%m-%d")
    df_filtrado["HORA_DESCARGA"] = now.strftime("%H:%M:%S")

    # 5. Guardar
    output_path = "data/processed/BASE_FINAL_GENERAL.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_filtrado.to_csv(output_path, index=False)

    logging.info(f"✅ CSV actualizado correctamente")
    return 0  # 👈 éxito

# ========================
# EJECUCIÓN
# ========================
if __name__ == "__main__":
    exit(main())