import os
import io
import logging
import requests
import pandas as pd
from datetime import datetime
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
        "curso perforante", "curso perforantes", "curso penetrante"
    ],
    "Arma blanca": [
        "arma blanca", "punzocortante", "punzocortantes", "punzo-cortante", "punzopenetrante",
        "punzo-penetrante", "punzo cortante", "punzocortopenetrante", "degüello", "deguello",
        "degollamiento", "herida cortante", "decapitación", "decapitacion", "cuchillo", "punzante",
        "punta y filo", "botella rota", "objeto cortante", "corto penetrante", "cortopenetrante",
        "contusocortante", "traumatismos cortantes"
    ],
    "Asfixia": [
        "estrangulación", "estrangulamiento", "estrangulacion", "asfixia", "asfiixia",
        "extrangulamiento", "ahorcamiento", "ahorcadura", "sofocacion", "ahogamiento"
    ]
}

# ========================
# FUNCIONES
# ========================
def download_csv(url: str, headers: dict) -> pd.DataFrame:
    logging.info("📥 Descargando datos desde SINADEF...")
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text), encoding="utf-8")
        logging.info("✅ Archivo descargado correctamente.")
        return df
    except Exception as e:
        logging.error(f"❌ Error al descargar o leer el archivo: {e}")
        raise

def filter_necropsia_data(data: pd.DataFrame) -> pd.DataFrame:
    filtered = data[
        (data['MUERTE_VIOLENTA'] == "HOMICIDIO") &
        (data['NECROPSIA'] == "SI SE REALIZÓ NECROPSIA") &
        (data['ANIO'] >= 2017)
    ]
    logging.info(f"✅ Filtrado completado: {filtered.shape[0]} registros encontrados.")
    return filtered

def clasificar_causa(causa: str) -> str:
    causa = str(causa).lower()
    
    # Reglas especiales por combinación de palabras clave
    if "perforante" in causa and "penetrante" in causa:
        return "Arma de fuego"
    if "perforantes" in causa and "penetrantes" in causa:
        return "Arma de fuego"
    if "peroforantes" in causa and "penetrantes" in causa:
        return "Arma de fuego"

    # Revisión con palabras clave del diccionario
    for categoria, palabras in KEYWORDS.items():
        if any(p in causa for p in palabras):
            return categoria

    return "Otra causa"

def clasificar_causa_row(row: pd.Series) -> str:
    columnas = ['DEBIDO_CAUSA_A', 'DEBIDO_CAUSA_B', 'DEBIDO_CAUSA_C',
                'DEBIDO_CAUSA_D', 'DEBIDO_CAUSA_E', 'DEBIDO_CAUSA_F']
    texto_completo = " ".join(str(row[col]) for col in columnas if pd.notnull(row[col]))
    return clasificar_causa(texto_completo)

def clasificar_edad(row: pd.Series) -> str:
    try:
        edad = float(row.get('EDAD', None))
    except (ValueError, TypeError):
        return "Sin clasificar"
    tiempo = str(row.get('TIEMPO_EDAD', "")).strip().upper()
    if tiempo in ["MESES", "MINUTOS"]:
        return "Niño"
    elif tiempo == "AÑOS":
        if 0 <= edad <= 11: return "Niño"
        elif 12 <= edad <= 17: return "Adolescente"
        elif 18 <= edad <= 29: return "Joven"
        elif 30 <= edad <= 59: return "Adulto"
        elif 60 <= edad <= 100: return "Adulto mayor"
    return "Sin clasificar"

# ========================
# FLUJO PRINCIPAL
# ========================
def main():
    logging.info("🚀 Iniciando procesamiento de datos SINADEF...")

    url = "https://files.minsa.gob.pe/s/Ae52gBAMf9aKEzK/download/SINADEF_DATOS_ABIERTOS.csv"
    headers = {"User-Agent": "Mozilla/5.0"}

    df = download_csv(url, headers)
    df_filtrado = filter_necropsia_data(df)

    df_filtrado["Grupo_Causa"] = df_filtrado.apply(clasificar_causa_row, axis=1)
    df_filtrado["EDADES"] = df_filtrado.apply(clasificar_edad, axis=1)

    # 🔠 Normalizar el campo SEXO
    df_filtrado["SEXO"] = df_filtrado["SEXO"].str.upper().str.strip()
    df_filtrado["SEXO"] = df_filtrado["SEXO"].replace({
        "FEMENINO": "Mujer",
        "MASCULINO": "Hombre"
    })

    # 🕒 Agregar columnas con fecha y hora de procesamiento
    now = datetime.now()
    df_filtrado["FECHA_DESCARGA"] = now.strftime("%Y-%m-%d")
    df_filtrado["HORA_DESCARGA"] = now.strftime("%H:%M:%S")

    # 💾 Guardar archivo final
    output_path = os.path.join("data", "processed", "BASE_FINAL_GENERAL.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_filtrado.to_csv(output_path, index=False)

    logging.info(f"📦 Archivo final guardado en: {output_path}")
    logging.info(f"🕓 Fecha y hora de descarga: {now.strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()