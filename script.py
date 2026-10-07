"""
script.py

Pipeline OS2 (Carabineros de Chile) en un solo archivo. Corre las nueve etapas
de principio a fin y deja los consolidados listos para analisis.

    Etapa 1  descarga de los Excel OS2 desde el portal de Carabineros
    Etapa 2  Excel a CSV (sin alterar contenido)
    Etapa 3  columnas canonicas (nombres normalizados, valores SIN normalizar)
    Etapa 4  normalizacion de caracteres, fecha, hora y territorio
    Etapa 5  resumen mensual
    Etapa 6  union por grupo, homologacion y revision de calidad
    Etapa 7  direccion unica para geolocalizar (log propio: data/direcciones.log)
    Etapa 8  coordenadas con OpenStreetMap (log propio: data/geolocalizacion.log)
    Etapa 9  exportacion compacta para el dashboard (dashboard/datos/siniestros_N.parquet)

Uso:
    python script.py                    corre las nueve etapas
    python script.py --desde 7          rehace direcciones y coordenadas
    python script.py --desde 8          solo rehace las coordenadas
    python script.py --desde 8 --osm-actualizar   vuelve a descargar OSM y reconstruye el indice
    python script.py --desde 6 --semilla 1234     repite las mismas muestras aleatorias de los logs
    python script.py --desde 9          solo regenera los datos del dashboard
    python script.py --skip-existing    omite descargas y conversiones ya hechas
    python script.py --desde 4          parte desde la etapa 4
    python script.py --desde 3 --hasta 4

Carpetas (todas bajo data/):
    raw/                       Excel descargados + manifest_os2.csv
    csv_raw/                   CSV tal como vienen en Excel (etapa 2)
    csv_canon/                 columnas canonicas, valores originales (etapa 3)
    csv_norm/                  valores normalizados (etapa 4)
    resumen_mensual.csv        resumen por periodo (etapa 5)
    union/                     consolidados siniestros.csv, personas.csv, vehiculos.csv
    union/columnas_sin_normalizar/   consolidados de la etapa 3, para auditoria
    direcciones/               archivos de depuracion de la etapa 7
    script.log                 bitacora de todas las corridas
    direcciones.log            bitacora de depuracion de direcciones
    osm/                       extracto de OpenStreetMap e indice (etapa 8)
    geolocalizacion/           archivos de depuracion de la etapa 8
    geolocalizacion.log        bitacora de depuracion de coordenadas
    alias_calles.csv           alias de calles mantenidos a mano para la etapa 8 (ver ALIAS_MANUAL)

Fuera de data/:
    dashboard/datos/           datos compactos del dashboard (etapa 9); van al repositorio
    dashboard/app.py           dashboard en Streamlit (streamlit run dashboard/app.py)

Decisiones de esquema en los consolidados finales:
    - resultado: solo "Muerto" ("Fallecido" se unifica en "Muerto").
    - tipo de siniestro: categorias estandar (Atropello, Colision, Choque,
      Volcadura, Caida, Otros), llevadas desde el codigo numerico cuando existe.
    - zona y sector se eliminan; urbano_rural se conserva y se hereda desde
      accidentes hacia personas y vehiculos.
    - la direccion queda en una sola columna: "direccion" (etapa 7).
    - coordenadas lat/lon con metodo y precision estimada, desde OpenStreetMap (etapa 8).
      Datos (c) colaboradores de OpenStreetMap, ODbL: citar la fuente al publicar.
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
import csv
import collections
import difflib
import functools
import importlib.util
import hashlib
import logging
import os
import random
import shutil
import tempfile
import re
import sys
import time
import unicodedata

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------
BASE_DIR  = Path(__file__).resolve().parent
DATA_DIR  = BASE_DIR / "data"
RAW_DIR   = DATA_DIR / "raw"
CSV_DIR   = DATA_DIR / "csv_raw"
CANON_DIR = DATA_DIR / "csv_canon"
NORM_DIR  = DATA_DIR / "csv_norm"
UNION_DIR = DATA_DIR / "union"
SIN_NORM_DIR = UNION_DIR / "columnas_sin_normalizar"
for _d in (RAW_DIR, CSV_DIR, CANON_DIR, NORM_DIR, UNION_DIR, SIN_NORM_DIR):
    _d.mkdir(parents=True, exist_ok=True)

PAGE_URL    = "https://www.carabineros.cl/transparencia/tproactiva/rpro_os2.html"
BASE_URL    = "https://www.carabineros.cl/transparencia/tproactiva/"
TIMEOUT_SEC = 90
MAX_WORKERS = 6
REINTENTOS  = 3

YEAR_MIN = 2010
YEAR_MAX = datetime.now().year

CSV_SEP  = ";"
ENCODING = "utf-8-sig"
SEED     = 42

ARGS = sys.argv[1:]
SKIP_EXISTING = "--skip-existing" in ARGS


def _arg_int(nombre: str, defecto: int) -> int:
    if nombre in ARGS and ARGS.index(nombre) + 1 < len(ARGS):
        return int(ARGS[ARGS.index(nombre) + 1])
    return defecto


DESDE = _arg_int("--desde", 1)
# Semilla de las muestras aleatorias de los logs: cambia en cada corrida y queda registrada;
# con --semilla N se repite exactamente la misma muestra.
SEMILLA_MUESTRA = _arg_int("--semilla", random.randint(1, 999_999))
HASTA = _arg_int("--hasta", 9)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_PATH = DATA_DIR / "script.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("script")

INICIO_CORRIDA = time.time()
RESUMEN_ETAPAS: dict[int, dict] = {}

# ---------------------------------------------------------------------------
# Constantes de dominio
# ---------------------------------------------------------------------------
GRUPOS = ("accidentes", "personas", "vehiculos")
GRUPO_UNION = {"accidentes": "siniestros", "personas": "personas", "vehiculos": "vehiculos"}

TAG_GRUPO  = {"acc": "accidentes", "sin": "accidentes", "perso": "personas", "veh": "vehiculos"}
STEM_GRUPO = {"siniestros": "accidentes", "accidentes": "accidentes",
              "personas": "personas", "vehiculos": "vehiculos"}

RE_MENSUAL = re.compile(r"os2/os2_(acc|sin|perso|veh)_(\d{4})_(0[1-9]|1[0-2])\.(xlsx|xls|xlsb)$", re.I)
RE_HIST    = re.compile(r"os2/os2_(acc|sin|perso|veh)_(\d{4})(?:_v(\d+))?\.(xlsx|xls|xlsb)$", re.I)
RE_CONSOL  = re.compile(r"os2/(\d{4})/([a-z]+)_(\d{4})\.(xlsx|xls|xlsb)$", re.I)
RE_ARCHIVO = re.compile(r"^(accidentes|personas|vehiculos)_(\d{4})(?:_(\d{2}))?\.(csv|xlsx|xls|xlsb)$", re.I)

ENGINE_MAP = {".xlsx": "openpyxl", ".xlsb": "pyxlsb", ".xls": "xlrd"}
VACIOS = {"", "nan", "none", "<na>", "null"}

MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
         "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
EXCEL_EPOCH = datetime(1899, 12, 30)

# Esquemas de la etapa 3 (columnas canonicas, valores originales)
S3_COMUN = ["id_accidente", "anio_archivo", "mes_archivo", "fecha", "mes", "hora",
            "cod_comuna", "comuna", "region", "zona"]
S3_SCHEMA = {
    "accidentes": S3_COMUN + [
        "sector", "urbano_rural", "tipo_accidente", "siniestros", "cod_causa", "causa",
        "calle_1", "calle_2", "ruta", "km", "frente_nro",
        "fallecidos", "graves", "menos_graves", "leves", "ilesos", "parte_nro", "tribunal"],
    "personas":   S3_COMUN + ["calidad", "sexo", "edad", "resultado"],
    "vehiculos":  S3_COMUN + ["tipo_vehiculo", "servicio"],
}

# Esquemas de la etapa 4 (normalizados; zona y sector ya no se arrastran)
S4_COMUN = ["id_accidente", "anio_archivo", "mes_archivo", "fecha", "mes", "hora",
            "cod_region", "region", "cod_provincia", "provincia", "cod_comuna", "comuna"]
S4_SCHEMA = {
    "accidentes": S4_COMUN + [
        "urbano_rural", "tipo_accidente", "siniestros", "cod_causa", "causa",
        "calle_1", "calle_2", "ruta", "km", "frente_nro",
        "fallecidos", "graves", "menos_graves", "leves", "ilesos", "parte_nro", "tribunal"],
    "personas":   S4_COMUN + ["calidad", "sexo", "edad", "resultado"],
    "vehiculos":  S4_COMUN + ["tipo_vehiculo", "servicio"],
}

# Esquemas finales (etapa 6)
FINAL_SCHEMA = {
    "accidentes": S4_COMUN + [
        "urbano_rural", "tipo_siniestro", "cod_tipo_accidente", "tipo_accidente",
        "cod_causa", "causa",
        "fallecidos", "graves", "menos_graves", "leves", "ilesos", "parte_nro", "tribunal"],
    "personas":   S4_COMUN + ["urbano_rural", "calidad", "sexo", "edad", "resultado"],
    "vehiculos":  S4_COMUN + ["urbano_rural", "tipo_vehiculo", "servicio"],
}

COLS_NUMERICAS = ["fallecidos", "graves", "menos_graves", "leves", "ilesos", "edad", "anio_archivo"]
COLS_ENTERAS_SUFIJO = ["id_accidente", "cod_causa", "parte_nro", "edad", "fallecidos", "graves",
                       "menos_graves", "leves", "ilesos", "km", "frente_nro", "anio_archivo",
                       "cod_comuna", "cod_tipo_accidente"]

# ---------------------------------------------------------------------------
# Tablas territoriales (CUT vigente, 346 comunas, 56 provincias, 16 regiones)
# Fuente: paquete chilemapas 0.4.1 (CRAN, datos de la BCN), codigos_territoriales y
# codigos_territoriales_16r para los 21 codigos de Nuble (region creada en 2018).
# ---------------------------------------------------------------------------
REGIONES = {
    "15": "Arica y Parinacota", "01": "Tarapaca", "02": "Antofagasta", "03": "Atacama",
    "04": "Coquimbo", "05": "Valparaiso", "13": "Metropolitana de Santiago",
    "06": "Libertador General Bernardo O'Higgins", "07": "Maule", "16": "Nuble",
    "08": "Biobio", "09": "La Araucania", "14": "Los Rios", "10": "Los Lagos",
    "11": "Aysen del General Carlos Ibanez del Campo",
    "12": "Magallanes y de la Antartica Chilena",
}
REGION_CLAVES = {  # palabra clave (sin tildes, minusculas) para reconocer la region por texto
    "arica": "15", "tarapaca": "01", "antofagasta": "02", "atacama": "03", "coquimbo": "04",
    "valparaiso": "05", "metropolitana": "13", "higgins": "06", "maule": "07", "nuble": "16",
    "biobio": "08", "araucania": "09", "rios": "14", "lagos": "10", "aysen": "11",
    "magallanes": "12",
}
ROMANOS = {"XV": "15", "I": "01", "II": "02", "III": "03", "IV": "04", "V": "05", "RM": "13",
           "XIII": "13", "VI": "06", "VII": "07", "XVI": "16", "VIII": "08", "IX": "09",
           "XIV": "14", "X": "10", "XI": "11", "XII": "12"}

PROVINCIAS = {
    "011": "Iquique", "014": "Tamarugal", "021": "Antofagasta", "022": "El Loa", "023": "Tocopilla",
    "031": "Copiapo", "032": "Chanaral", "033": "Huasco", "041": "Elqui", "042": "Choapa",
    "043": "Limari", "051": "Valparaiso", "052": "Isla de Pascua", "053": "Los Andes",
    "054": "Petorca", "055": "Quillota", "056": "San Antonio", "057": "San Felipe de Aconcagua",
    "058": "Marga Marga", "061": "Cachapoal", "062": "Cardenal Caro", "063": "Colchagua",
    "071": "Talca", "072": "Cauquenes", "073": "Curico", "074": "Linares", "081": "Concepcion",
    "082": "Arauco", "083": "Biobio", "091": "Cautin", "092": "Malleco", "101": "Llanquihue",
    "102": "Chiloe", "103": "Osorno", "104": "Palena", "111": "Coihaique", "112": "Aisen",
    "113": "Capitan Prat", "114": "General Carrera", "121": "Magallanes", "122": "Antartica Chilena",
    "123": "Tierra del Fuego", "124": "Ultima Esperanza", "131": "Santiago", "132": "Cordillera",
    "133": "Chacabuco", "134": "Maipo", "135": "Melipilla", "136": "Talagante", "141": "Valdivia",
    "142": "Ranco", "151": "Arica", "152": "Parinacota", "161": "Diguillin", "162": "Itata",
    "163": "Punilla",
}

# Codigos antiguos de Nuble (cuando pertenecia a la region del Biobio) a codigo vigente
OLD_NUBLE = {"08401": "16101", "08402": "16102", "08403": "16202", "08404": "16203", "08405": "16302", "08406": "16103", "08407": "16104", "08408": "16204", "08409": "16303", "08410": "16105", "08411": "16106", "08412": "16205", "08413": "16107", "08414": "16201", "08415": "16206", "08416": "16301", "08417": "16304", "08418": "16108", "08419": "16305", "08420": "16207", "08421": "16109"}

COMUNAS_CUT = """\
01101 Iquique
01107 Alto Hospicio
01401 Pozo Almonte
01402 Camina
01403 Colchane
01404 Huara
01405 Pica
02101 Antofagasta
02102 Mejillones
02103 Sierra Gorda
02104 Taltal
02201 Calama
02202 Ollague
02203 San Pedro de Atacama
02301 Tocopilla
02302 Maria Elena
03101 Copiapo
03102 Caldera
03103 Tierra Amarilla
03201 Chanaral
03202 Diego de Almagro
03301 Vallenar
03302 Alto del Carmen
03303 Freirina
03304 Huasco
04101 La Serena
04102 Coquimbo
04103 Andacollo
04104 La Higuera
04105 Paiguano
04106 Vicuna
04201 Illapel
04202 Canela
04203 Los Vilos
04204 Salamanca
04301 Ovalle
04302 Combarbala
04303 Monte Patria
04304 Punitaqui
04305 Rio Hurtado
05101 Valparaiso
05102 Casablanca
05103 Concon
05104 Juan Fernandez
05105 Puchuncavi
05107 Quintero
05109 Vina del Mar
05201 Isla de Pascua
05301 Los Andes
05302 Calle Larga
05303 Rinconada
05304 San Esteban
05401 La Ligua
05402 Cabildo
05403 Papudo
05404 Petorca
05405 Zapallar
05501 Quillota
05502 Calera
05503 Hijuelas
05504 La Cruz
05506 Nogales
05601 San Antonio
05602 Algarrobo
05603 Cartagena
05604 El Quisco
05605 El Tabo
05606 Santo Domingo
05701 San Felipe
05702 Catemu
05703 Llaillay
05704 Panquehue
05705 Putaendo
05706 Santa Maria
05801 Quilpue
05802 Limache
05803 Olmue
05804 Villa Alemana
06101 Rancagua
06102 Codegua
06103 Coinco
06104 Coltauco
06105 Donihue
06106 Graneros
06107 Las Cabras
06108 Machali
06109 Malloa
06110 Mostazal
06111 Olivar
06112 Peumo
06113 Pichidegua
06114 Quinta de Tilcoco
06115 Rengo
06116 Requinoa
06117 San Vicente
06201 Pichilemu
06202 La Estrella
06203 Litueche
06204 Marchihue
06205 Navidad
06206 Paredones
06301 San Fernando
06302 Chepica
06303 Chimbarongo
06304 Lolol
06305 Nancagua
06306 Palmilla
06307 Peralillo
06308 Placilla
06309 Pumanque
06310 Santa Cruz
07101 Talca
07102 Constitucion
07103 Curepto
07104 Empedrado
07105 Maule
07106 Pelarco
07107 Pencahue
07108 Rio Claro
07109 San Clemente
07110 San Rafael
07201 Cauquenes
07202 Chanco
07203 Pelluhue
07301 Curico
07302 Hualane
07303 Licanten
07304 Molina
07305 Rauco
07306 Romeral
07307 Sagrada Familia
07308 Teno
07309 Vichuquen
07401 Linares
07402 Colbun
07403 Longavi
07404 Parral
07405 Retiro
07406 San Javier
07407 Villa Alegre
07408 Yerbas Buenas
08101 Concepcion
08102 Coronel
08103 Chiguayante
08104 Florida
08105 Hualqui
08106 Lota
08107 Penco
08108 San Pedro de la Paz
08109 Santa Juana
08110 Talcahuano
08111 Tome
08112 Hualpen
08201 Lebu
08202 Arauco
08203 Canete
08204 Contulmo
08205 Curanilahue
08206 Los Alamos
08207 Tirua
08301 Los Angeles
08302 Antuco
08303 Cabrero
08304 Laja
08305 Mulchen
08306 Nacimiento
08307 Negrete
08308 Quilaco
08309 Quilleco
08310 San Rosendo
08311 Santa Barbara
08312 Tucapel
08313 Yumbel
08314 Alto Biobio
09101 Temuco
09102 Carahue
09103 Cunco
09104 Curarrehue
09105 Freire
09106 Galvarino
09107 Gorbea
09108 Lautaro
09109 Loncoche
09110 Melipeuco
09111 Nueva Imperial
09112 Padre las Casas
09113 Perquenco
09114 Pitrufquen
09115 Pucon
09116 Saavedra
09117 Teodoro Schmidt
09118 Tolten
09119 Vilcun
09120 Villarrica
09121 Cholchol
09201 Angol
09202 Collipulli
09203 Curacautin
09204 Ercilla
09205 Lonquimay
09206 Los Sauces
09207 Lumaco
09208 Puren
09209 Renaico
09210 Traiguen
09211 Victoria
10101 Puerto Montt
10102 Calbuco
10103 Cochamo
10104 Fresia
10105 Frutillar
10106 Los Muermos
10107 Llanquihue
10108 Maullin
10109 Puerto Varas
10201 Castro
10202 Ancud
10203 Chonchi
10204 Curaco de Velez
10205 Dalcahue
10206 Puqueldon
10207 Queilen
10208 Quellon
10209 Quemchi
10210 Quinchao
10301 Osorno
10302 Puerto Octay
10303 Purranque
10304 Puyehue
10305 Rio Negro
10306 San Juan de la Costa
10307 San Pablo
10401 Chaiten
10402 Futaleufu
10403 Hualaihue
10404 Palena
11101 Coihaique
11102 Lago Verde
11201 Aisen
11202 Cisnes
11203 Guaitecas
11301 Cochrane
11302 O'Higgins
11303 Tortel
11401 Chile Chico
11402 Rio Ibanez
12101 Punta Arenas
12102 Laguna Blanca
12103 Rio Verde
12104 San Gregorio
12201 Cabo de Hornos
12202 Antartica
12301 Porvenir
12302 Primavera
12303 Timaukel
12401 Natales
12402 Torres del Paine
13101 Santiago
13102 Cerrillos
13103 Cerro Navia
13104 Conchali
13105 El Bosque
13106 Estacion Central
13107 Huechuraba
13108 Independencia
13109 La Cisterna
13110 La Florida
13111 La Granja
13112 La Pintana
13113 La Reina
13114 Las Condes
13115 Lo Barnechea
13116 Lo Espejo
13117 Lo Prado
13118 Macul
13119 Maipu
13120 Nunoa
13121 Pedro Aguirre Cerda
13122 Penalolen
13123 Providencia
13124 Pudahuel
13125 Quilicura
13126 Quinta Normal
13127 Recoleta
13128 Renca
13129 San Joaquin
13130 San Miguel
13131 San Ramon
13132 Vitacura
13201 Puente Alto
13202 Pirque
13203 San Jose de Maipo
13301 Colina
13302 Lampa
13303 Tiltil
13401 San Bernardo
13402 Buin
13403 Calera de Tango
13404 Paine
13501 Melipilla
13502 Alhue
13503 Curacavi
13504 Maria Pinto
13505 San Pedro
13601 Talagante
13602 El Monte
13603 Isla de Maipo
13604 Padre Hurtado
13605 Penaflor
14101 Valdivia
14102 Corral
14103 Lanco
14104 Los Lagos
14105 Mafil
14106 Mariquina
14107 Paillaco
14108 Panguipulli
14201 La Union
14202 Futrono
14203 Lago Ranco
14204 Rio Bueno
15101 Arica
15102 Camarones
15201 Putre
15202 General Lagos
16101 Chillan
16102 Bulnes
16103 Chillan Viejo
16104 El Carmen
16105 Pemuco
16106 Pinto
16107 Quillon
16108 San Ignacio
16109 Yungay
16201 Quirihue
16202 Cobquecura
16203 Coelemu
16204 Ninhue
16205 Portezuelo
16206 Ranquil
16207 Treguaco
16301 San Carlos
16302 Coihueco
16303 Niquen
16304 San Fabian
16305 San Nicolas
"""

ALIAS_COMUNAS = {  # clave del dato (sin tildes ni signos) -> clave de la comuna en la tabla
    "aysen": "aisen", "coyhaique": "coihaique", "llayllay": "llaillay", "lacalera": "calera",
    "paguirrecerda": "pedroaguirrecerda", "sanpedroatacama": "sanpedrodeatacama",
    "marchigue": "marchihue", "trehuaco": "treguaco", "tiltil": "tiltil", "iladepascua": "isladepascua",
    "cabodehornosexnavarino": "cabodehornos", "navarino": "cabodehornos",
}

# ---------------------------------------------------------------------------
# Auxiliares generales
# ---------------------------------------------------------------------------
def duracion(seg: float) -> str:
    m, s = divmod(int(seg), 60)
    return f"{m} min {s:02d} s"


def miles(n) -> str:
    return f"{int(n):,}".replace(",", ".")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_archivo(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for bloque in iter(lambda: f.read(65536), b""):
            h.update(bloque)
    return h.hexdigest()


def nombre_clave(name: str) -> str:
    """Nombre de columna en minusculas, sin tildes ni signos."""
    s = str(name).strip().lower().replace("\n", " ").replace("\t", " ")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^0-9a-z]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def escribir_csv_seguro(df: pd.DataFrame, destino: Path) -> None:
    """
    Escribe a un temporal y reemplaza el destino con reintentos. Evita el
    PermissionError que aparece cuando OneDrive o Excel tienen el archivo abierto.
    """
    tmp = destino.with_name(destino.name + ".tmp")
    df.to_csv(tmp, index=False, sep=CSV_SEP, encoding=ENCODING)
    for intento in range(1, 7):
        try:
            os.replace(tmp, destino)
            return
        except PermissionError:
            log.warning("Archivo bloqueado (%s), reintento %d de 6", destino.name, intento)
            time.sleep(5)
    raise PermissionError(f"No se pudo reemplazar {destino}. Cierra el archivo o pausa OneDrive.")


def leer_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=CSV_SEP, dtype=str, encoding=ENCODING, low_memory=False)


def sufijo_periodo(anio: int, mes: str | None) -> str:
    return f"{anio}_{mes}" if mes else f"{anio}"


def listar_archivos(carpeta: Path, ext: tuple[str, ...]) -> list[tuple[Path, str, int, str | None]]:
    """Devuelve (ruta, grupo, anio, mes|None) ordenado por grupo, anio y mes."""
    salida = []
    for p in carpeta.iterdir():
        m = RE_ARCHIVO.match(p.name.lower())
        if m and m.group(4) in ext:
            salida.append((p, m.group(1), int(m.group(2)), m.group(3)))
    return sorted(salida, key=lambda t: (GRUPOS.index(t[1]), t[2], t[3] or ""))


# ===========================================================================
# ETAPA 1: descarga
# ===========================================================================
def fetch_html(url: str) -> str:
    for intento in range(1, REINTENTOS + 1):
        try:
            r = requests.get(url, timeout=TIMEOUT_SEC)
            r.raise_for_status()
            return r.text
        except Exception as exc:
            log.warning("Fallo al leer el indice (intento %d): %s", intento, exc)
            time.sleep(3 * intento)
    raise RuntimeError("No se pudo leer la pagina de indice del portal OS2.")


def parse_candidatos(html: str) -> list[dict]:
    hrefs = sorted(set(re.findall(r'href="(OS2/[^"]+\.(?:xlsx|xls|xlsb))"', html, flags=re.I)))
    log.info("Links Excel detectados: %d", len(hrefs))
    if not hrefs:
        raise RuntimeError("No se encontraron links a Excel OS2. Posible cambio en el portal.")

    cand = []
    for href in hrefs:
        h = href.lower()
        m = RE_MENSUAL.search(h)
        if m:
            cand.append({"href": href, "grupo": TAG_GRUPO[m.group(1)], "anio": int(m.group(2)),
                         "mes": m.group(3), "version": 1, "prioridad": 1})
            continue
        m = RE_CONSOL.search(h)
        if m:
            carpeta, stem, anio = int(m.group(1)), m.group(2), int(m.group(3))
            if STEM_GRUPO.get(stem) and carpeta == anio:
                cand.append({"href": href, "grupo": STEM_GRUPO[stem], "anio": anio,
                             "mes": None, "version": 999, "prioridad": 2})
            continue
        m = RE_HIST.search(h)
        if m:
            cand.append({"href": href, "grupo": TAG_GRUPO[m.group(1)], "anio": int(m.group(2)),
                         "mes": None, "version": int(m.group(3) or 1), "prioridad": 1})
    cand = [c for c in cand if YEAR_MIN <= c["anio"] <= YEAR_MAX]
    log.info("Candidatos validos: %d", len(cand))
    return cand


def seleccionar(cand: list[dict]) -> list[dict]:
    """Un archivo anual por (grupo, anio); mensuales solo si no existe el anual."""
    anuales: dict[tuple, dict] = {}
    for c in cand:
        if c["mes"] is None:
            k = (c["grupo"], c["anio"])
            if k not in anuales or (c["prioridad"], c["version"]) > (anuales[k]["prioridad"], anuales[k]["version"]):
                anuales[k] = c
    mensuales: dict[tuple, dict] = {}
    for c in sorted(cand, key=lambda x: x["href"]):
        if c["mes"] is None:
            continue
        if (c["grupo"], c["anio"]) in anuales:
            log.info("Mensual ignorado (existe archivo anual): %s", c["href"])
            continue
        mensuales[(c["grupo"], c["anio"], c["mes"])] = c
    sel = sorted(list(anuales.values()) + list(mensuales.values()),
                 key=lambda c: (c["anio"], GRUPOS.index(c["grupo"]), c["mes"] or ""))
    log.info("Seleccionados: %d (anuales=%d, mensuales=%d)", len(sel), len(anuales), len(mensuales))
    meses: dict[tuple, list[int]] = {}
    for (g, a, m) in mensuales:
        meses.setdefault((a, g), []).append(int(m))
    for (a, g), lst in sorted(meses.items()):
        log.info("  %d %s: meses %s", a, g, sorted(lst))
    return sel


def destino_raw(item: dict) -> Path:
    ext = Path(item["href"]).suffix.lower()
    return RAW_DIR / f"{item['grupo']}_{sufijo_periodo(item['anio'], item['mes'])}{ext}"


def descargar(item: dict) -> dict:
    url, path = BASE_URL + item["href"], destino_raw(item)
    base = {"grupo": item["grupo"], "anio": item["anio"], "mes": item["mes"] or "",
            "href": item["href"], "url": url, "local": str(path)}
    if SKIP_EXISTING and path.exists() and path.stat().st_size > 0:
        sha = sha256_archivo(path)
        log.info("Ya existe, omitido: %s", path.name)
        return {**base, "status": "omitido", "bytes": path.stat().st_size, "sha256": sha}
    ultimo = None
    for intento in range(1, REINTENTOS + 1):
        try:
            r = requests.get(url, timeout=TIMEOUT_SEC)
            r.raise_for_status()
            if len(r.content) < 1000:
                raise ValueError(f"respuesta demasiado pequena ({len(r.content)} bytes)")
            path.write_bytes(r.content)
            sha = sha256_bytes(r.content)
            log.info("Descargado: %s (%d bytes) sha256=%s...", path.name, len(r.content), sha[:12])
            return {**base, "status": "descargado", "bytes": len(r.content), "sha256": sha}
        except Exception as exc:
            ultimo = exc
            time.sleep(3 * intento)
    log.error("Fallo al descargar %s: %s", url, ultimo)
    return {**base, "status": f"failed: {ultimo}", "bytes": 0, "sha256": ""}


if DESDE <= 1 <= HASTA:
    _t0 = time.time()
    log.info("=== ETAPA 1: descarga (%s) ===", PAGE_URL)
    _items = seleccionar(parse_candidatos(fetch_html(PAGE_URL)))
    _res = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as _ex:
        for _f in as_completed([_ex.submit(descargar, i) for i in _items]):
            _res.append(_f.result())
    _res.sort(key=lambda r: (r["anio"], r["grupo"], r["mes"]))
    with (RAW_DIR / "manifest_os2.csv").open("w", newline="", encoding="utf-8") as _f:
        _w = csv.DictWriter(_f, fieldnames=["grupo", "anio", "mes", "href", "url", "local", "status", "bytes", "sha256"])
        _w.writeheader()
        _w.writerows(_res)
    RESUMEN_ETAPAS[1] = {
        "nombre": "descarga",
        "ok": sum(r["status"] == "descargado" for r in _res),
        "omitidos": sum(r["status"] == "omitido" for r in _res),
        "fallidos": sum(r["status"].startswith("failed") for r in _res),
        "seg": time.time() - _t0,
    }

# ===========================================================================
# ETAPA 2: Excel a CSV
# ===========================================================================
def leer_excel(path: Path) -> pd.DataFrame:
    engine = ENGINE_MAP.get(path.suffix.lower())
    if engine is None:
        raise ValueError(f"Extension no soportada: {path.suffix}")
    return pd.read_excel(path, sheet_name=0, dtype=str, engine=engine, header=0)


if DESDE <= 2 <= HASTA:
    _t0 = time.time()
    log.info("=== ETAPA 2: Excel a CSV ===")
    _excel = listar_archivos(RAW_DIR, ("xlsx", "xls", "xlsb"))
    log.info("Archivos Excel encontrados: %d", len(_excel))
    _ok = _om = _fa = 0
    for _src, _grupo, _anio, _mes in _excel:
        _dst = CSV_DIR / f"{_grupo}_{sufijo_periodo(_anio, _mes)}.csv"
        if SKIP_EXISTING and _dst.exists() and _dst.stat().st_size > 0 and _dst.stat().st_mtime >= _src.stat().st_mtime:
            log.info("Ya existe, omitido: %s", _dst.name)
            _om += 1
            continue
        try:
            _df = leer_excel(_src)
            _df = _df.dropna(how="all")
            _unnamed = [c for c in _df.columns if str(c).strip().lower().startswith("unnamed")]
            if _unnamed:
                log.warning("Columnas Unnamed eliminadas en %s: %s", _src.name, _unnamed)
                _df = _df.drop(columns=_unnamed)
            escribir_csv_seguro(_df, _dst)
            log.info("Convertido: %s -> %s (%d filas, %d columnas)", _src.name, _dst.name, len(_df), len(_df.columns))
            _ok += 1
        except Exception as exc:
            log.error("Fallo al convertir %s: %s", _src.name, exc)
            _fa += 1
    RESUMEN_ETAPAS[2] = {"nombre": "excel a csv", "ok": _ok, "omitidos": _om, "fallidos": _fa,
                         "seg": time.time() - _t0}

# ===========================================================================
# ETAPA 3: columnas canonicas (valores sin normalizar)
# ===========================================================================
ALIAS = {
    "id_accidente":   ["id_accidente", "idaccidente", "id"],
    "fecha":          ["fecha"],
    "mes":            ["mes"],
    "hora":           ["hora"],
    "region":         ["region"],
    "zona":           ["zona"],
    "sector":         ["sector"],
    "urbano_rural":   ["urbano_rural", "urbanorural"],
    "tipo_accidente": ["tipoaccdte", "tipo_accdte", "accdtes", "tipo_accidente", "tipo_de_accidente", "tipo"],
    "siniestros":     ["siniestros"],
    "fallecidos":     ["fallecidos", "fallecido", "muertos"],
    "graves":         ["graves", "grave"],
    "menos_graves":   ["m_grave", "menos_graves", "menos_grave", "mgrave"],
    "leves":          ["leves", "leve"],
    "ilesos":         ["ilesos", "ileso"],
    "calle_1":        ["calle_1", "calleuno"],
    "calle_2":        ["calle_2", "calledos"],
    "ruta":           ["rolruta", "ruta"],
    "km":             ["km", "ubicacionkm", "ubicacion_km"],
    "frente_nro":     ["frentenumero", "frte_nro", "frente_nro", "frente_numero"],
    "parte_nro":      ["parte_nro", "parte"],
    "tribunal":       ["tribunal"],
    "calidad":        ["calidad"],
    "sexo":           ["sexo"],
    "edad":           ["edad"],
    "resultado":      ["resultado"],
    "tipo_vehiculo":  ["tipo_vehiculo", "tipo"],
    "servicio":       ["servicio"],
}


def limpiar_vacios(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    return s.mask(s.str.lower().isin(VACIOS))


def a_canonico(df: pd.DataFrame, grupo: str, anio: int, mes: str | None):
    cm: dict[str, list] = {}
    for c in df.columns:
        if not str(c).strip().lower().startswith("unnamed"):
            cm.setdefault(nombre_clave(c), []).append(c)
    usadas: set[str] = set()
    mapeo: list[str] = []
    vacia = lambda: pd.Series(pd.NA, index=df.index, dtype="string")  # noqa: E731

    def tomar(canon: str, *claves: str) -> pd.Series:
        for k in claves:
            if k in cm:
                usadas.add(k)
                mapeo.append(f"{cm[k][0]} -> {canon}")
                return limpiar_vacios(df[cm[k][0]])
        return vacia()

    n = len(df)
    out = {c: vacia() for c in S3_SCHEMA[grupo]}
    out["anio_archivo"] = pd.Series([str(anio)] * n, index=df.index, dtype="string")
    out["mes_archivo"] = pd.Series([mes] * n, index=df.index, dtype="string") if mes else vacia()

    for canon in ("id_accidente", "fecha", "mes", "hora", "region", "zona"):
        out[canon] = tomar(canon, *ALIAS[canon])

    # Comuna y codigo de comuna segun las columnas disponibles en cada anio
    if "nomcomuna" in cm and "comuna" in cm:
        out["cod_comuna"], out["comuna"] = tomar("cod_comuna", "comuna"), tomar("comuna", "nomcomuna")
    elif "comuna2" in cm and "comuna" in cm:
        out["cod_comuna"], out["comuna"] = tomar("cod_comuna", "comuna"), tomar("comuna", "comuna2")
    elif "comunas" in cm and "comuna" in cm:
        out["cod_comuna"], out["comuna"] = tomar("cod_comuna", "comuna"), tomar("comuna", "comunas")
    elif "codcomuna" in cm and "comuna" in cm:
        out["cod_comuna"], out["comuna"] = tomar("cod_comuna", "codcomuna"), tomar("comuna", "comuna")
    elif "cod_comuna" in cm and "comuna" in cm:
        out["cod_comuna"], out["comuna"] = tomar("cod_comuna", "cod_comuna"), tomar("comuna", "comuna")
    elif "comuna" in cm:
        out["comuna"] = tomar("comuna", "comuna")

    if grupo == "accidentes":
        for canon in ("sector", "urbano_rural", "tipo_accidente", "siniestros", "fallecidos", "graves",
                      "menos_graves", "leves", "ilesos", "calle_1", "calle_2", "ruta", "km",
                      "frente_nro", "parte_nro", "tribunal"):
            out[canon] = tomar(canon, *ALIAS[canon])
        if out["urbano_rural"].isna().all() and "sector" in cm:  # archivos 2025 en adelante
            out["urbano_rural"], out["sector"] = tomar("urbano_rural", "sector"), vacia()
        if "causa" in cm and "causas" in cm:
            out["cod_causa"], out["causa"] = tomar("cod_causa", "causa"), tomar("causa", "causas")
        elif "causas" in cm:
            out["causa"] = tomar("causa", "causas")
        elif "causa" in cm:
            out["causa"] = tomar("causa", "causa")
    elif grupo == "personas":
        for canon in ("calidad", "sexo", "edad", "resultado"):
            out[canon] = tomar(canon, *ALIAS[canon])
    else:
        for canon in ("tipo_vehiculo", "servicio"):
            out[canon] = tomar(canon, *ALIAS[canon])

    res = pd.DataFrame(out)[S3_SCHEMA[grupo]]
    desconocidas = {k: v for k, v in cm.items() if k not in usadas}
    return res, mapeo, desconocidas


if DESDE <= 3 <= HASTA:
    _t0 = time.time()
    log.info("=== ETAPA 3: columnas canonicas (valores sin normalizar) ===")
    _csvs = listar_archivos(CSV_DIR, ("csv",))
    log.info("Archivos a procesar: %d", len(_csvs))
    _ok = _fa = 0
    for _p, _grupo, _anio, _mes in _csvs:
        try:
            _df = leer_csv(_p)
            _out, _mapeo, _desc = a_canonico(_df, _grupo, _anio, _mes)
            for _k, _v in _desc.items():
                log.warning("Columna DESCONOCIDA (revisar): archivo=%s col_norm='%s' (orig=%s)", _p.name, _k, _v)
            _sin_datos = [c for c in _out.columns if c not in ("anio_archivo", "mes_archivo") and _out[c].isna().all()]
            if _sin_datos:
                log.warning("Campos canonicos sin datos: archivo=%s campos=%s", _p.name, _sin_datos)
            escribir_csv_seguro(_out, CANON_DIR / _p.name)
            log.info("Escrito: %s (%d filas, %d columnas)", _p.name, len(_out), len(_out.columns))
            log.info("  mapeo %s: %s", _p.name, " | ".join(_mapeo))
            _ok += 1
        except Exception as exc:
            log.error("Fallo al procesar %s: %s", _p.name, exc)
            _fa += 1
    RESUMEN_ETAPAS[3] = {"nombre": "columnas canonicas", "ok": _ok, "omitidos": 0, "fallidos": _fa,
                         "seg": time.time() - _t0}

# ===========================================================================
# ETAPA 4: normalizacion de caracteres, fecha, hora y territorio
# ===========================================================================
RE_NTILDE   = re.compile(r"i\u00bf(?:1[\u2044/]2|\u00bd)")          # "Castai?1/2os" -> "Castanos"
RE_I_AGUDA1 = re.compile(r"(?<=[A-Za-z])i\u00a1(?=[A-Za-z])")             # "Henri¡Quez" -> "Henriquez"
RE_I_AGUDA2 = re.compile(r"(?<=[A-Za-z])\u00a1(?=[A-Za-z])")              # "R¡Erez" -> "Rierez"
RE_NTILDE2  = re.compile(r"(?<=[A-Za-z])\u00b1(?=[A-Za-z])")        # "Ermita±Os"
RE_APOSTR   = re.compile(r"(?<=[A-Za-z])[\u00b7\u2019\u2018\u00b4`](?=[A-Za-z])")  # "O·Higgins"
RE_DIGITO_MAYUS = re.compile(r"(?<=\d)([A-Z])")


def normalizar_texto(x):
    """
    Repara codificaciones rotas comunes, quita tildes (la enie queda como n),
    colapsa espacios y deja Title Case. Devuelve None si queda vacio.
    """
    if x is None or (not isinstance(x, str) and pd.isna(x)):
        return None
    t = str(x)
    if "\u00c3" in t or "\u00c2" in t:  # UTF-8 leido como cp1252 o latin-1
        for codec in ("cp1252", "latin-1"):
            try:
                t = t.encode(codec).decode("utf-8")
                break
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
    t = RE_NTILDE.sub("\u00f1", t)
    t = RE_I_AGUDA1.sub("\u00ed", t)
    t = RE_I_AGUDA2.sub("\u00ed", t)
    t = RE_NTILDE2.sub("\u00f1", t)
    t = t.replace("\u00a5", "\u00f1")
    t = re.sub(r"(?<=[A-Za-z])\u00be(?=[A-Za-z])", "o", t)   # "Quell¾n" -> "Quellon"
    t = re.sub(r"^\u00a1(?=[A-Za-z])", "", t)                  # "¡H-30" -> "H-30"
    t = t.strip(" |")
    t = RE_APOSTR.sub("'", t)
    t = t.replace("\ufffd", "").replace("\u00a0", " ")
    t = unicodedata.normalize("NFKD", t)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = re.sub(r"\s+", " ", t).strip()
    if not t or t.lower() in VACIOS:
        return None
    t = t.lower().title()
    return RE_DIGITO_MAYUS.sub(lambda m: m.group(1).lower(), t)


def aplicar_por_unicos(s: pd.Series, fn) -> pd.Series:
    """Aplica fn a los valores unicos y reconstruye la serie (rapido en columnas repetitivas)."""
    s = s.astype("string")
    unicos = s.dropna().unique()
    mapa = {u: fn(u) for u in unicos}
    return s.map(lambda v: mapa.get(v) if isinstance(v, str) else None).astype("string")


def quitar_sufijo_entero(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.replace(r"^(-?\d+)\.0+$", r"\1", regex=True)


def parse_fecha(x):
    t = str(x).strip()
    try:
        if re.fullmatch(r"\d{4,6}(?:\.\d+)?", t):
            serie = float(t)
            if 20000 < serie < 80000:
                return (EXCEL_EPOCH + timedelta(days=int(serie))).strftime("%Y-%m-%d")
            return None
        m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", t) or re.match(r"^(\d{4})/(\d{1,2})/(\d{1,2})", t)
        if m:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
        m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})", t)
        if m:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
    except ValueError:
        return None
    return None


def parse_hora(x):
    t = str(x).strip()
    if re.fullmatch(r"\d*\.?\d+(?:[eE]-?\d+)?", t):
        v = float(t)
        if 0 <= v < 1:
            minutos = int(round(v * 1440)) % 1440
            return f"{minutos // 60:02d}:{minutos % 60:02d}"
        return None
    m = re.search(r"(\d{1,2}):(\d{2})", t)
    if m and int(m.group(1)) < 24 and int(m.group(2)) < 60:
        return f"{int(m.group(1)):02d}:{m.group(2)}"
    return None


# --- territorio ---------------------------------------------------------------
def clave_territorio(x: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (normalizar_texto(x) or "").lower())


TABLA_CUT: dict[str, tuple] = {}
for _l in COMUNAS_CUT.strip().splitlines():
    _cut, _nombre = _l.split(" ", 1)
    TABLA_CUT[_cut] = (normalizar_texto(_nombre), _cut[:3], normalizar_texto(PROVINCIAS[_cut[:3]]),
                       _cut[:2], normalizar_texto(REGIONES[_cut[:2]]))
NOMBRE_A_CUT = {clave_territorio(v[0]): k for k, v in TABLA_CUT.items()}
CLAVES_COMUNAS = list(NOMBRE_A_CUT)


def region_desde_texto(txt):
    if txt is None:
        return None
    t = str(txt).strip()
    if re.fullmatch(r"\d{1,2}", t) and t.zfill(2) in REGIONES:
        return t.zfill(2)
    if t.upper() in ROMANOS:
        return ROMANOS[t.upper()]
    k = clave_territorio(t)
    for palabra, cod in REGION_CLAVES.items():
        if palabra in k:
            return cod
    return None


def cut_por_codigo(v):
    """CUT a partir de un codigo (con ceros a la izquierda perdidos o codigo antiguo de Nuble)."""
    if v is None:
        return None
    t = str(v).strip()
    if not re.fullmatch(r"\d{4,5}(?:\.0)?", t):
        return None
    cut = re.sub(r"\.0$", "", t).zfill(5)
    cut = OLD_NUBLE.get(cut, cut)
    return cut if cut in TABLA_CUT else None


def cut_por_nombre(v):
    """CUT a partir del nombre. Devuelve (cut, tipo) con tipo exacto, alias o aproximado (umbral 0,85)."""
    if v is None or re.fullmatch(r"\d+(?:\.0)?", str(v).strip()):
        return None, None
    k = clave_territorio(v)
    if k in NOMBRE_A_CUT:
        return NOMBRE_A_CUT[k], "exacto"
    k2 = ALIAS_COMUNAS.get(k)
    if k2 in NOMBRE_A_CUT:
        return NOMBRE_A_CUT[k2], "alias"
    cercano = difflib.get_close_matches(k, CLAVES_COMUNAS, n=1, cutoff=0.85)
    return (NOMBRE_A_CUT[cercano[0]], "aproximado") if cercano else (None, None)


def resolver_comuna(cod, nom):
    """
    Devuelve (cut|None, via, codigo_inexistente, discrepancia).
    El nombre manda cuando se reconoce de forma exacta o por alias, porque aparece en todos los
    anios y en 2020 a 2024 el codigo de origen incluye valores que no son CUT (1103, 5106, etc.).
    El codigo se usa si el nombre falta o solo calza de forma aproximada; la coincidencia
    aproximada es el ultimo recurso.
    """
    n_cut, n_tipo = cut_por_nombre(nom)
    if n_cut is None:
        n_cut, n_tipo = cut_por_nombre(cod)
    c_cut = cut_por_codigo(cod) or cut_por_codigo(nom)
    hay_num = any(v is not None and re.fullmatch(r"\d{4,5}(?:\.0)?", str(v).strip()) for v in (cod, nom))
    inexistente = hay_num and c_cut is None
    discrepa = bool(c_cut and n_cut and c_cut != n_cut)
    if n_cut and n_tipo != "aproximado":
        return n_cut, n_tipo, inexistente, discrepa
    if c_cut:
        return c_cut, "codigo", inexistente, discrepa
    if n_cut:
        return n_cut, "aproximado", inexistente, discrepa
    return None, None, inexistente, discrepa


def resolver_territorio(df: pd.DataFrame, archivo: str) -> pd.DataFrame:
    cod = df["cod_comuna"].astype("string")
    nom = df["comuna"].astype("string")
    reg = df["region"].astype("string")
    llave = cod.fillna("") + "\x1f" + nom.fillna("") + "\x1f" + reg.fillna("")
    mapa, inexist_llaves, discrepancias, aproximados = {}, set(), {}, {}
    for k in llave.unique():
        c, n, r = [p or None for p in k.split("\x1f")]
        cut, via, inexistente, discrepa = resolver_comuna(c, n)
        if inexistente:
            inexist_llaves.add(k)
        if discrepa:
            discrepancias[k] = (cut_por_codigo(c) or cut_por_codigo(n), cut)
        if via == "aproximado":
            aproximados[k] = cut
        if cut:
            nombre, cp, prov, cr, regn = TABLA_CUT[cut]
            mapa[k] = (cr, regn, cp, prov, cut, nombre)
        else:
            rc = region_desde_texto(r)
            mapa[k] = (rc, normalizar_texto(REGIONES[rc]) if rc else normalizar_texto(r),
                       None, None, None, normalizar_texto(n) if n and not re.fullmatch(r"\d+(?:\.0)?", n) else None)
    res = pd.DataFrame(llave.map(mapa).tolist(), index=df.index,
                       columns=["cod_region", "region", "cod_provincia", "provincia", "cod_comuna", "comuna"])
    res = res.astype("string")
    pct = res["cod_comuna"].notna().mean() * 100 if len(res) else 0
    log.info("  territorio %s: %.1f%% de las filas con codigo de comuna", archivo, pct)
    n_inex = int(llave.isin(inexist_llaves).sum())
    if n_inex:
        ejemplos = sorted({k.split("\x1f")[0] or k.split("\x1f")[1] for k in inexist_llaves})[:5]
        log.info("  %s: %s filas con codigo de origen que no es un CUT vigente (se resuelven por nombre). Ej: %s",
                 archivo, miles(n_inex), ejemplos)
    if discrepancias:
        n_dis = int(llave.isin(discrepancias).sum())
        ej = [f"{k.split(chr(31))[0]}|{k.split(chr(31))[1]}: codigo={TABLA_CUT[v[0]][0]} nombre={TABLA_CUT[v[1]][0]}"
              for k, v in list(discrepancias.items())[:5]]
        log.warning("%s: %s filas donde el codigo y el nombre de comuna apuntan a comunas distintas (se usa el nombre). Ej: %s",
                    archivo, miles(n_dis), ej)
    if aproximados:
        n_apr = int(llave.isin(aproximados).sum())
        ej = [f"{(k.split(chr(31))[1] or k.split(chr(31))[0])} -> {TABLA_CUT[v][0]}" for k, v in list(aproximados.items())[:5]]
        log.warning("%s: %s filas resueltas por coincidencia aproximada de nombre. Ej: %s", archivo, miles(n_apr), ej)
    sin = res["cod_comuna"].isna()
    if sin.any():
        top = df.loc[sin, "comuna"].fillna("<vacio>").value_counts().head(5).to_dict()
        log.warning("%s: %s filas sin comuna resuelta (principales: %s)", archivo, miles(int(sin.sum())), top)
    return res


# --- normalizacion de un archivo ---------------------------------------------
COLS_TEXTO = {
    "accidentes": ["urbano_rural", "tipo_accidente", "siniestros", "causa", "calle_1", "calle_2", "ruta", "tribunal"],
    "personas":   ["calidad", "sexo", "resultado"],
    "vehiculos":  ["tipo_vehiculo", "servicio"],
}


def normalizar_archivo(df: pd.DataFrame, grupo: str, archivo: str):
    out = pd.DataFrame(index=df.index)
    cambios: dict[str, int] = {}

    def registrar(col: str, antes: pd.Series, despues: pd.Series):
        n = int((antes.fillna("\x00") != despues.fillna("\x00")).sum())
        if n:
            cambios[col] = n

    for c in ("id_accidente", "anio_archivo", "mes_archivo"):
        out[c] = quitar_sufijo_entero(df[c])

    out["fecha"] = aplicar_por_unicos(df["fecha"], parse_fecha)
    registrar("fecha", df["fecha"].astype("string"), out["fecha"])
    out["hora"] = aplicar_por_unicos(df["hora"], parse_hora)
    registrar("hora", df["hora"].astype("string"), out["hora"])
    for col, ori in (("fecha", df["fecha"]), ("hora", df["hora"])):
        malos = int((ori.notna() & out[col].isna()).sum())
        if malos:
            log.warning("  %s: %s valores de %s no se pudieron interpretar (ej: %s)", archivo, miles(malos), col,
                        ori[ori.notna() & out[col].isna()].unique()[:3].tolist())

    # mes: se deriva de la fecha; si no hay fecha se usa el texto de origen
    mes_f = out["fecha"].str[5:7].map(lambda m: MESES[int(m) - 1] if isinstance(m, str) and m.isdigit() and 1 <= int(m) <= 12 else None)
    mes_o = aplicar_por_unicos(df["mes"], lambda v: (MESES[int(float(v)) - 1] if re.fullmatch(r"\d{1,2}(?:\.0)?", str(v).strip()) and 1 <= int(float(v)) <= 12 else normalizar_texto(v)))
    out["mes"] = mes_f.astype("string").fillna(mes_o)
    registrar("mes", df["mes"].astype("string"), out["mes"])

    terr = resolver_territorio(df, archivo)
    for c in terr.columns:
        out[c] = terr[c]
    registrar("comuna", df["comuna"].astype("string"), out["comuna"])

    for c in COLS_TEXTO[grupo]:
        out[c] = aplicar_por_unicos(df[c], normalizar_texto)
        registrar(c, df[c].astype("string"), out[c])

    if "urbano_rural" in out.columns:
        out["urbano_rural"] = out["urbano_rural"].map(
            lambda v: {"U": "Urbano", "R": "Rural"}.get(v, v) if isinstance(v, str) else None).astype("string")

    for c in S4_SCHEMA[grupo]:
        if c in out.columns:
            continue
        s = df[c] if c in df.columns else pd.Series(pd.NA, index=df.index, dtype="string")
        out[c] = quitar_sufijo_entero(s) if c in COLS_ENTERAS_SUFIJO else s.astype("string")
    if grupo == "personas":
        _raw = df["resultado"].astype("string").str.strip()
        _muertes = {v: int(n) for v, n in _raw.value_counts().items() if re.search(r"fallec|muert", str(v).lower())}
        log.info("  resultado de origen (fallecidos): %s", _muertes)
    if grupo == "accidentes":  # parte_nro: sin signos sueltos ni espacios dobles
        out["parte_nro"] = (out["parte_nro"].str.replace("\u00ba", "\u00b0", regex=False)
                            .str.replace(r"^\u00a1", "", regex=True).str.replace(r"\s+", " ", regex=True).str.strip())
    if grupo == "personas":  # "Fallecido" y "Muerto" son la misma categoria
        _fallecido = out["resultado"].str.lower().eq("fallecido").fillna(False).astype(bool)
        out.loc[_fallecido, "resultado"] = "Muerto"
        _vacios = int(out["resultado"].isna().sum())
        if _vacios:
            log.info("  %s: %s personas con resultado vacio (se mantienen vacias)", archivo, miles(_vacios))
    return out[S4_SCHEMA[grupo]], cambios


if DESDE <= 4 <= HASTA:
    _t0 = time.time()
    log.info("=== ETAPA 4: normalizacion de caracteres ===")
    _canon = listar_archivos(CANON_DIR, ("csv",))
    log.info("Archivos a procesar: %d", len(_canon))
    _ok = _fa = 0
    for _p, _grupo, _anio, _mes in _canon:
        try:
            _df = leer_csv(_p)
            _out, _cambios = normalizar_archivo(_df, _grupo, _p.name)
            escribir_csv_seguro(_out, NORM_DIR / _p.name)
            log.info("Escrito: %s (%d filas, %s celdas modificadas)", _p.name, len(_out), miles(sum(_cambios.values())))
            log.info("  detalle por columna: %s", _cambios)
            _ok += 1
        except Exception as exc:
            log.error("Fallo al normalizar %s: %s", _p.name, exc)
            _fa += 1
    RESUMEN_ETAPAS[4] = {"nombre": "caracteres", "ok": _ok, "omitidos": 0, "fallidos": _fa,
                         "seg": time.time() - _t0}

# ===========================================================================
# ETAPA 5: resumen mensual
# ===========================================================================
if DESDE <= 5 <= HASTA:
    _t0 = time.time()
    log.info("=== ETAPA 5: resumen mensual ===")
    _norm = listar_archivos(NORM_DIR, ("csv",))
    log.info("Archivos a resumir: %d", len(_norm))
    _tabla: dict[str, list] = {}
    _num = ["fallecidos", "graves", "menos_graves", "leves", "ilesos"]
    _ok = _fa = 0
    for _p, _grupo, _anio, _mes in _norm:
        try:
            _df = leer_csv(_p)
            _df["periodo"] = _df["fecha"].str[:7]
            _filas = _df.groupby("periodo").size()
            if _grupo == "accidentes":
                for _c in _num:
                    _df[_c] = pd.to_numeric(_df[_c], errors="coerce").fillna(0)
                _g = _df.groupby("periodo")[_num].sum()
                _g.insert(0, "accidentes_filas", _filas)
            else:
                _g = _filas.to_frame(f"{_grupo}_filas")
            _tabla.setdefault(_grupo, []).append(_g)
            _ok += 1
        except Exception as exc:
            log.error("Fallo al resumir %s: %s", _p.name, exc)
            _fa += 1
    _partes = [pd.concat(v).groupby(level=0).sum() for v in _tabla.values()]
    if _partes:
        _res = pd.concat(_partes, axis=1).fillna(0).astype(int).sort_index()
        _res.index.name = "periodo"
        _res = _res.reset_index()
        _orden = ["periodo", "accidentes_filas"] + _num + ["personas_filas", "vehiculos_filas"]
        _res = _res[[c for c in _orden if c in _res.columns]]
        escribir_csv_seguro(_res, DATA_DIR / "resumen_mensual.csv")
        log.info("Escrito: resumen_mensual.csv (%d periodos)", len(_res))
        log.info("Ultimos 12 periodos:\n%s", _res.tail(12).to_string(index=False))
    RESUMEN_ETAPAS[5] = {"nombre": "resumen mensual", "ok": 1 if _partes else 0, "omitidos": 0,
                         "fallidos": _fa, "seg": time.time() - _t0}

# ===========================================================================
# ETAPA 6: union por grupo, homologacion y revision
# ===========================================================================
TIPO_PRIMERA_PALABRA = {"atropello": "Atropello", "colision": "Colision", "choque": "Choque",
                        "volcadura": "Volcadura", "caida": "Caida",
                        "otro": "Otros", "impacto": "Otros", "incendio": "Otros"}


def homologar_tipo_siniestro(sin: pd.DataFrame) -> pd.DataFrame:
    """tipo_siniestro con categorias estandar; el codigo numerico se conserva aparte."""
    tipo = sin["tipo_accidente"].astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
    cod = tipo.where(tipo.str.fullmatch(r"\d+").fillna(False))
    texto = sin["siniestros"].astype("string").str.strip().combine_first(tipo.where(cod.isna()))
    prim = (texto.str.normalize("NFKD").str.replace(r"[\u0300-\u036f]", "", regex=True)
                 .str.lower().str.split().str[0].astype("string"))
    macro = prim.map(TIPO_PRIMERA_PALABRA).astype("string")
    macro = macro.where(prim.isna() | macro.notna(), "Otros")

    desconocidos = texto[(prim.notna() & ~prim.isin(list(TIPO_PRIMERA_PALABRA)) & prim.ne("otros")).fillna(False)].value_counts().head(20)
    if len(desconocidos):
        log.warning("Etiquetas de tipo asignadas a 'Otros' por defecto:\n%s", desconocidos.to_string())

    # El mapa codigo -> tipo se aprende de las filas que traen ambos datos
    ap = pd.DataFrame({"cod": cod, "macro": macro.where(sin["siniestros"].notna())}).dropna()
    if len(ap):
        ct = pd.crosstab(ap["cod"], ap["macro"])
        mapa = ct.idxmax(axis=1)
        pureza = ct.max(axis=1) / ct.sum(axis=1)
        log.info("Mapa codigo a tipo de siniestro (aprendido de las filas con ambos datos):\n%s",
                 pd.concat([mapa.rename("tipo"), ct.sum(axis=1).rename("n"), pureza.round(3).rename("pureza")],
                           axis=1).to_string())
        if (pureza < 1).any():
            log.warning("Codigos que apuntan a mas de un tipo: %s", list(pureza[pureza < 1].index))
        macro_cod = cod.map(mapa.to_dict()).astype("string")
    else:
        log.warning("Ninguna fila trae a la vez codigo y categoria: no se puede aprender el mapa de codigos.")
        macro_cod = pd.Series(pd.NA, index=sin.index, dtype="string")

    sin = sin.copy()
    sin["tipo_siniestro"] = macro.combine_first(macro_cod)
    sin["cod_tipo_accidente"] = cod
    sin["tipo_accidente"] = tipo.where(cod.isna())
    sin_mapa = sorted(set(cod.dropna()) - (set(mapa.index) if len(ap) else set()))
    log.info("Filas sin tipo_siniestro: %s | codigos sin mapa: %s",
             miles(int(sin["tipo_siniestro"].isna().sum())), sin_mapa)
    return sin


def muestra_por_anio(df: pd.DataFrame, nombre: str, por_anio: int = 1, bloque: int = 6, ancho: int = 26) -> pd.DataFrame:
    """
    Muestra aleatoria estratificada por anio (cada anio trae un formato de origen distinto),
    impresa en el log como tabla traspuesta: una fila por columna y una columna por registro.
    """
    m = pd.concat([g.sample(n=min(por_anio, len(g)), random_state=SEMILLA_MUESTRA)
                   for _, g in df.groupby("anio_archivo")])
    log.info("MUESTRA ALEATORIA %s: %d registros, %d por anio (semilla %d)", nombre, len(m), por_anio, SEMILLA_MUESTRA)
    t = m.astype("string").fillna("<vacio>")
    t.index = [f"{a}#{i}" for a, i in zip(m["anio_archivo"], m["id_accidente"])]
    t = t.drop(columns=["anio_archivo", "id_accidente"]).T
    t = t.apply(lambda col: col.str.slice(0, ancho))
    for i in range(0, t.shape[1], bloque):
        log.info("\n%s", t.iloc[:, i:i + bloque].to_string())
    return m


def perfil(df: pd.DataFrame, nombre: str):
    """Perfil por columna, observaciones de formato y registro en el log."""
    mb = (UNION_DIR / nombre).stat().st_size / 1024 / 1024 if (UNION_DIR / nombre).exists() else 0
    log.info("REVISION %s: %s filas, %d columnas, %.1f MB", nombre, miles(len(df)), df.shape[1], mb)
    por_anio = df["anio_archivo"].value_counts().sort_index()
    log.info("Filas por anio: %s", " | ".join(f"{a}: {miles(n)}" for a, n in por_anio.items()))
    filas, completos, obs = [], [], []
    for c in df.columns:
        s = df[c]
        pct = s.notna().mean() * 100
        nun = s.nunique()
        if nun > 2000:
            dist, ejemplo = ">2000", "ej: " + " | ".join(s.dropna().head(3).astype(str))
        else:
            vc = s.value_counts()
            dist = miles(nun)
            ejemplo = " | ".join(f"{v} ({miles(n)})" for v, n in vc.head(7).items())
            if nun <= 40:
                completos.append(f"  {c} ({nun}): " + " | ".join(f"{v} ({miles(n)})" for v, n in vc.items()))
        filas.append({"columna": c, "pct_completo": round(pct, 1), "distintos": dist, "ejemplos": ejemplo})
        if c in ("anio_archivo", "mes_archivo"):
            continue
        sd = s.dropna().astype(str)
        if c in COLS_NUMERICAS:
            malos = sd[~sd.str.fullmatch(r"-?\d+(?:\.\d+)?")]
            if len(malos):
                obs.append((c, "valor no numerico", len(malos), malos.unique()[:3].tolist()))
        else:
            malos = sd[sd.str.contains(r"[^\x00-\x7F\u00b0]", regex=True)]
            if len(malos):
                obs.append((c, "caracteres con tilde o no ASCII", len(malos), malos.unique()[:3].tolist()))
            malos = sd[sd.str.contains(r"\u00c3|\u00c2|\ufffd|^\s|\s$|\s{2,}", regex=True)]
            if len(malos):
                obs.append((c, "mojibake o espacios sobrantes", len(malos), malos.unique()[:3].tolist()))
    tabla = pd.DataFrame(filas)
    ancho = max(len(c) for c in tabla["columna"]) + 2
    cab = f"{'columna':<{ancho}}{'% completo':>10}  {'distintos':>9}  mas frecuentes / ejemplos"
    cuerpo = "\n".join(f"{r.columna:<{ancho}}{r.pct_completo:>10.1f}  {r.distintos:>9}  {r.ejemplos[:150]}"
                       for r in tabla.itertuples())
    log.info("%s\n%s", cab, cuerpo)
    if completos:
        log.info("Valores completos de las columnas con 40 categorias o menos:\n%s", "\n".join(completos))
    for c, tipo, n, ej in obs:
        log.warning("OBSERVACION %s | %s: %s | %s filas | ej: %s", nombre, c, tipo, miles(n), " | ".join(ej))
    if not obs:
        log.info("REVISION %s: sin observaciones de formato", nombre)
    tabla.insert(0, "consolidado", nombre)
    obs_df = pd.DataFrame([{"consolidado": nombre, "columna": c, "tipo": t, "filas": n, "ejemplos": " | ".join(e)}
                           for c, t, n, e in obs])
    return tabla, obs_df, len(obs)


if DESDE <= 6 <= HASTA:
    _t0 = time.time()
    log.info("=== ETAPA 6: union de archivos por grupo ===")
    _datos: dict[str, pd.DataFrame] = {}
    _ok = _fa = 0

    # 6.1 concatenar los archivos normalizados (y la copia sin normalizar para auditoria)
    for _grupo in GRUPOS:
        try:
            _partes = [(_p, _a, _m) for _p, _g, _a, _m in listar_archivos(NORM_DIR, ("csv",)) if _g == _grupo]
            if not _partes:
                raise RuntimeError(f"No hay archivos normalizados de {_grupo} en {NORM_DIR}")
            _datos[_grupo] = pd.concat([leer_csv(_p) for _p, _, _ in _partes], ignore_index=True)
            _anios = sorted({a for _, a, _ in _partes})
            _crudos = [(_p, _a, _m) for _p, _g, _a, _m in listar_archivos(CANON_DIR, ("csv",)) if _g == _grupo]
            if _crudos:
                _sn = pd.concat([leer_csv(_p) for _p, _, _ in _crudos], ignore_index=True)
                escribir_csv_seguro(_sn, SIN_NORM_DIR / f"{GRUPO_UNION[_grupo]}.csv")
                log.info("Escrito: union\\columnas_sin_normalizar\\%s.csv (%d archivos, %d filas, anios %d a %d)",
                         GRUPO_UNION[_grupo], len(_crudos), len(_sn), _anios[0], _anios[-1])
                del _sn
            log.info("Leido: %s (%d archivos, %d filas, anios %d a %d)", _grupo, len(_partes),
                     len(_datos[_grupo]), _anios[0], _anios[-1])
        except Exception as exc:
            log.error("Fallo al unir %s: %s: %s", _grupo, type(exc).__name__, exc)
            _fa += 1

    if "accidentes" in _datos:
        _sin = _datos["accidentes"]

        # 6.2 tipo de siniestro estandar (la direccion se arma en la etapa 7)
        _sin = homologar_tipo_siniestro(_sin)
        _datos["accidentes"] = _sin[FINAL_SCHEMA["accidentes"]]

        # 6.3 urbano_rural heredado desde accidentes hacia personas y vehiculos
        _dup = int(_sin.duplicated(["anio_archivo", "id_accidente"]).sum())
        log.info("Accidentes con id repetido dentro del anio: %s", miles(_dup))
        _ur = _sin[["anio_archivo", "id_accidente", "urbano_rural"]].drop_duplicates(["anio_archivo", "id_accidente"])
        for _grupo in ("personas", "vehiculos"):
            if _grupo not in _datos:
                continue
            _g = _datos[_grupo].drop(columns=["urbano_rural"], errors="ignore")
            _g = _g.merge(_ur, on=["anio_archivo", "id_accidente"], how="left")
            _datos[_grupo] = _g[FINAL_SCHEMA[_grupo]]
            log.info("%s con urbano_rural: %.1f %%", _grupo, _datos[_grupo]["urbano_rural"].notna().mean() * 100)

    # 6.4 cuadratura entre accidentes y personas (despues de unificar Fallecido en Muerto)
    if "accidentes" in _datos and "personas" in _datos:
        _pares = [("fallecidos", "Muerto"), ("graves", "Grave"), ("menos_graves", "Menos Grave"),
                  ("leves", "Leve"), ("ilesos", "Ileso")]
        _clave = ["anio_archivo", "id_accidente"]
        _detalle = []
        for _col, _res in _pares:
            _a = (_datos["accidentes"][_clave].assign(v=pd.to_numeric(_datos["accidentes"][_col], errors="coerce").fillna(0))
                  .groupby(_clave)["v"].sum())
            _b = _datos["personas"].loc[_datos["personas"]["resultado"] == _res, _clave].groupby(_clave).size()
            _d = pd.concat([_a.rename("accidentes"), _b.rename("personas")], axis=1).fillna(0).astype(int)
            _d["diferencia"] = _d["personas"] - _d["accidentes"]
            _dif = _d[_d["diferencia"] != 0]
            _tot = int(_d["diferencia"].sum())
            (log.info if _tot == 0 else log.warning)(
                "Cuadratura %s: accidentes=%s | personas(%s)=%s | diferencia=%s | accidentes con diferencia=%s",
                _col, miles(_d["accidentes"].sum()), _res, miles(_d["personas"].sum()), miles(_tot), miles(len(_dif)))
            if len(_dif):
                _por_anio = _dif.groupby(level=0)["diferencia"].agg(["size", "sum"])
                log.info("  %s por anio (accidentes con diferencia | suma de diferencias): %s", _col,
                         " | ".join(f"{a}: {int(r['size'])}/{int(r['sum']):+d}" for a, r in _por_anio.iterrows()))
                _ej = _dif.reset_index().head(5)
                log.info("  %s ejemplos (anio, id, accidentes, personas): %s", _col,
                         [tuple(r) for r in _ej[["anio_archivo", "id_accidente", "accidentes", "personas"]].itertuples(index=False)])
                _detalle.append(_dif.reset_index().assign(categoria=_col))
        if _detalle:
            pd.concat(_detalle)[["categoria", "anio_archivo", "id_accidente", "accidentes", "personas", "diferencia"]].to_csv(
                UNION_DIR / "cuadratura_detalle.csv", index=False, sep=CSV_SEP, encoding=ENCODING)
            log.info("Detalle de diferencias: %s", UNION_DIR / "cuadratura_detalle.csv")

    # 6.4b reparar texto danado en tribunal usando valores limpios y frecuentes como referencia
    if "accidentes" in _datos:
        _t = _datos["accidentes"]["tribunal"].astype("string")
        _sucio = _t.str.contains(r"[^\x00-\x7F\u00b0]", regex=True, na=False)
        if _sucio.any():
            _frec = _t[~_sucio & _t.notna()].value_counts()
            _cands = list(_frec[_frec >= 3].index)
            _mapa_t, _vecino, _ej_t = {}, 0, []
            for _v in _t[_sucio].unique():
                _limpio = re.sub(r"\s+", " ", re.sub(r"[^\x00-\x7F\u00b0]", "", _v)).strip()
                _cerca = difflib.get_close_matches(_limpio, _cands, n=1, cutoff=0.92)
                _mapa_t[_v] = _cerca[0] if _cerca else _limpio
                _vecino += bool(_cerca)
                if len(_ej_t) < 3:
                    _ej_t.append(f"{_v} -> {_mapa_t[_v]}")
            _datos["accidentes"]["tribunal"] = _t.where(~_sucio, _t.map(_mapa_t))
            log.info("Tribunal con caracteres danados: %s filas, %d valores distintos (%d reparados con un valor vecino). Ej: %s",
                     miles(int(_sucio.sum())), len(_mapa_t), _vecino, _ej_t)

    # 6.5 escritura y revision
    _perfiles, _obs_all, _resumen_salida = [], [], []
    for _grupo in GRUPOS:
        if _grupo not in _datos:
            continue
        _nombre = f"{GRUPO_UNION[_grupo]}.csv"
        try:
            escribir_csv_seguro(_datos[_grupo], UNION_DIR / _nombre)
            _anios = sorted(_datos[_grupo]["anio_archivo"].dropna().unique())
            log.info("Escrito: union\\%s (%d filas, anios %s a %s)", _nombre, len(_datos[_grupo]), _anios[0], _anios[-1])
            _tabla_p, _obs_df, _n_obs = perfil(_datos[_grupo], _nombre)
            if _grupo != "accidentes":  # siniestros se muestrea al cierre, ya con direccion y coordenadas
                muestra_por_anio(_datos[_grupo], _nombre).to_csv(
                    UNION_DIR / f"muestra_aleatoria_{GRUPO_UNION[_grupo]}.csv", index=False, sep=CSV_SEP, encoding=ENCODING)
            _perfiles.append(_tabla_p)
            _obs_all.append(_obs_df)
            _datos[_grupo].sample(n=min(1000, len(_datos[_grupo])), random_state=SEED).to_csv(
                UNION_DIR / f"muestra_{GRUPO_UNION[_grupo]}.csv", index=False, sep=CSV_SEP, encoding=ENCODING)
            _resumen_salida.append((_nombre, len(_datos[_grupo]), (UNION_DIR / _nombre).stat().st_size / 1024 / 1024, _n_obs))
            _ok += 1
        except Exception as exc:
            log.error("Fallo al unir union\\%s: %s: %s", _nombre, type(exc).__name__, exc)
            _fa += 1
    if _perfiles:
        pd.concat(_perfiles).to_csv(UNION_DIR / "perfil_columnas.csv", index=False, sep=CSV_SEP, encoding=ENCODING)
        pd.concat([o for o in _obs_all if len(o)] or [pd.DataFrame(columns=["consolidado", "columna", "tipo", "filas", "ejemplos"])]
                  ).to_csv(UNION_DIR / "observaciones.csv", index=False, sep=CSV_SEP, encoding=ENCODING)
        log.info("Para revisar a mano: %s, %s y muestra_*.csv", UNION_DIR / "perfil_columnas.csv", UNION_DIR / "observaciones.csv")
    log.info("RESUMEN DE SALIDAS\n%s", "\n".join(
        [f"{'consolidado':<20}{'filas':>12}{'MB':>10}{'observaciones':>15}"] +
        [f"{n:<20}{miles(f):>12}{mb:>10.1f}{o:>15}" for n, f, mb, o in _resumen_salida]))
    for _grupo in GRUPOS:
        _ruta = UNION_DIR / f"{GRUPO_UNION[_grupo]}.csv"
        if _ruta.exists():
            _edad = time.time() - _ruta.stat().st_mtime
            if _ruta.stat().st_mtime >= INICIO_CORRIDA:
                log.info("Consolidado ACTUALIZADO: %s (%.1f MB, generado hace %d s)", _ruta, _ruta.stat().st_size / 1024 / 1024, _edad)
            else:
                log.warning("Consolidado NO ACTUALIZADO en esta corrida: %s (ultima escritura hace %s)", _ruta, duracion(_edad))
    RESUMEN_ETAPAS[6] = {"nombre": "union", "ok": _ok, "omitidos": 0, "fallidos": _fa, "seg": time.time() - _t0}

# ===========================================================================
# ETAPA 7: direccion unica para geolocalizar (con log propio de depuracion)
# ===========================================================================
# Formatos de salida. Se pueden cambiar aqui sin tocar el resto del codigo.
FMT_INTERSECCION = "{c1} & {c2}"        # Avenida Pedro de Valdivia & Avenida Irarrazaval
FMT_NUMERO       = "{c1} {n}"           # Avenida Providencia 1234
FMT_RUTA_KM      = "Ruta {r}, km {k}"   # Ruta 5, km 27
FMT_RUTA         = "Ruta {r}"           # Ruta 68
FMT_VIA_KM       = "{c1}, km {k}"       # Camino a Lonquen, km 12
SENT = "<<sin_dato>>"                   # marca interna de componente vacio
COMPONENTES_DIR = ["via_1", "via_2", "numero", "ruta_cod", "km_val"]   # se guardan para la etapa 8

DIR_DIR = DATA_DIR / "direcciones"
DIR_DIR.mkdir(parents=True, exist_ok=True)
DIR_LOG_PATH = DATA_DIR / "direcciones.log"
dlog = logging.getLogger("direcciones")
dlog.setLevel(logging.INFO)
dlog.propagate = False
if not dlog.handlers:
    _fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    for _h in (logging.FileHandler(DIR_LOG_PATH, encoding="utf-8"), logging.StreamHandler()):
        _h.setFormatter(_fmt)
        dlog.addHandler(_h)

# Tipos de via: cualquier variante (sin punto, minusculas) -> forma escrita completa
TIPOS_VIA = {
    "av": "Avenida", "avd": "Avenida", "avda": "Avenida", "avenida": "Avenida", "aven": "Avenida",
    "pje": "Pasaje", "psje": "Pasaje", "psj": "Pasaje", "pasj": "Pasaje", "pj": "Pasaje", "pasaje": "Pasaje",
    "cam": "Camino", "cno": "Camino", "cmno": "Camino", "camino": "Camino",
    "cll": "Calle", "clle": "Calle", "calle": "Calle",
    "autop": "Autopista", "autopista": "Autopista",
    "ctra": "Carretera", "carr": "Carretera", "carretera": "Carretera",
    "pob": "Poblacion", "pobl": "Poblacion", "poblacion": "Poblacion",
    "vla": "Villa", "villa": "Villa", "sector": "Sector", "costanera": "Costanera", "diag": "Diagonal", "diagonal": "Diagonal",
    "circ": "Circunvalacion", "circunvalacion": "Circunvalacion", "pza": "Plaza", "plaza": "Plaza",
}
# Titulos y abreviaturas frecuentes en nombres de calles (solo como palabra completa)
ABREVIATURAS = {
    "gral": "General", "grl": "General", "gnrl": "General", "pdte": "Presidente", "presid": "Presidente",
    "sta": "Santa", "sto": "Santo", "cdte": "Comandante", "cmdte": "Comandante", "tte": "Teniente",
    "cap": "Capitan", "capt": "Capitan", "cnel": "Coronel", "crl": "Coronel", "sgto": "Sargento",
    "dr": "Doctor", "dra": "Doctora", "prof": "Profesor", "mons": "Monsenor", "ing": "Ingeniero",
    "arz": "Arzobispo", "hnos": "Hermanos", "bdo": "Bernardo", "libert": "Libertador", "lib": "Libertador",
    "liber": "Libertador", "ohiggins": "O'Higgins", "almte": "Almirante", "alm": "Almirante",
    "mcal": "Mariscal", "obpo": "Obispo", "pbro": "Presbitero", "ffcc": "Ferrocarril",
    "cost": "Costanera", "card": "Cardenal", "alc": "Alcalde", "nva": "Nueva", "nvo": "Nuevo",
    "crnel": "Coronel", "profe": "Profesor", "rtda": "Rotonda", "esc": "Escuela", "arqto": "Arquitecto",
    "arq": "Arquitecto", "rep": "Republica", "diag": "Diagonal", "carabs": "Carabineros", "jj": "Jose Joaquin", "jm": "Jose Miguel", "avda": "Avenida", "avd": "Avenida",
}
PARTICULAS = {"de", "del", "y", "e", "a", "al", "en"}         # siempre en minuscula dentro del nombre
ARTICULOS = {"el", "la", "los", "las", "lo"}                    # en minuscula solo tras una particula
POI_PALABRAS = re.compile(r"\b(?:estacionamientos?|estac|mall|supermercado|superm|servicentro|hospital|terminal|"
                          r"discoteque|discoteca|carcel|colegio|escuela|liceo|universidad|clinica|consultorio|"
                          r"restaurant|local comercial|centro comercial|copec|shell|petrobras|bomba)\b", re.I)
RE_PREFIJO_LUGAR = re.compile(r"^(?:frente\s+(?:a\s+|al\s+)?|interior\s+(?:de\s+|del\s+)?|al\s+lado\s+del?\s+|"
                              r"(?:a\s+la\s+)?altura\s+(?:de\s+|del\s+)?)", re.I)
ROMANO = re.compile(r"^(?=[ivxlc]+$)c{0,3}(xc|xl|l?x{0,3})(ix|iv|v?i{0,3})$", re.I)
VACIOS_DIR = {"sin informacion", "sin nombre", "s/n", "sn", "s/i", "se ignora", "no indica", "desconocido",
              "sin datos", "sin dato", "xx", "xxx", "no", "nn", "n/n", "0", "-", ".", "x", "sin calle"}
GENERICOS = {"camino publico", "camino vecinal", "camino interior", "camino rural", "camino",
             "calle", "pasaje", "avenida", "ruta"}
SIN_ROL = {"s/r", "sr", "s/rol", "sin rol", "s/n", "sn", "0", "-", "s r", "s.r"}
FECHA_EXCEL = (36526, 47483)      # numeros de serie de Excel entre 2000-01-01 y 2029-12-31
RE_NUM_MARCADO = re.compile(r"\s*(?:,\s*)?(?:N\u00b0|N\u00ba|N\.?\s?o\.?|Nro\.?|Num\.?|Numero|#)\s*(\d{1,5}[A-Za-z]?)\b\.?\s*$", re.I)
RE_ESQUINA     = re.compile(r"\s+(?:esq\.?|esquina|c/|&)\s+|(?<!\d)\s*/\s*(?!\d)", re.I)   # "16 1/2 Norte" no se parte
RE_CON         = re.compile(r"\s+con\s+(?!con\b)", re.I)
RE_SUFIJO_TIPO = re.compile(r"^(.*?)\s*=>\s*(.*)$")
RE_GUION_TIPO  = re.compile(r"^(.*\S)\s+-\s+(avda|avenida|av|calle|pasaje|psje|pje|camino|cam)\.?$", re.I)
RE_SECTOR      = re.compile(r"^(.*?),?\s*\bsector\b\s*:?\s*(.*)$", re.I)
RE_KM_EN_TEXTO = re.compile(r"\bkm\.?\s*(\d{1,4}(?:[.,]\d{1,3})?)\b", re.I)
RE_ALTURA      = re.compile(r"(?:\b(?:a\s+la\s+)?altura(?:\s+del?)?|\bfrente(?:\s+al?)?)\s*$", re.I)
RE_PARENTESIS  = re.compile(r"\s*\([^)]*(?:\)|$)\s*")
RE_FECHA_TXT   = re.compile(r"^\d{4}-\d{2}-\d{2}")
RE_RUTA_COD    = re.compile(r"^(?:(?:ruta|rta\.?)\s+)?([a-z]{1,2})\s*-?\s*(\d{1,3})\b", re.I)
RE_RUTA_NUM    = re.compile(r"^(?:(?:ruta|rta\.?)\s+)?(\d{1,3})(?:\s*(?:norte|sur))?\b", re.I)
RE_LONGITUDINAL = re.compile(r"^(?:longitudinal|panamericana|carretera panamericana)\s*(norte|sur)?\b", re.I)
# Deteccion estricta de una ruta escrita en la calle ("Ruta 68", "Carretera Ruta 68", "Autopista 5 Sur",
# "Caletera Ruta 5 Sur", "E-71", "S30"); "2 Norte" o "8 Sur" de Talca no califican.
RE_RUTA_EN_CALLE = re.compile(
    r"^(?:(?:carretera|autopista|caletera|camino)\s+)?(?:ruta|rta\.?)\s+(?P<a>[a-z]{1,2}\s*-?\s*\d{1,3}|\d{1,3})\b"
    r"|^(?:autopista|carretera)\s+(?P<b>\d{1,3})\b"
    r"|^(?:caletera|carretera|autopista)\s+(?P<g>[a-z]{1,2}\s*-\s*\d{1,3})\b"
    r"|^(?P<c>[a-z]{1,2})\s*-\s*(?P<d>\d{2,3})\b|^(?P<e>[a-z])\s?(?P<f>\d{2,3})$", re.I)
VIA_RURAL = re.compile(r"^(?:camino|carretera|ruta|longitudinal|panamericana|autopista|caletera|sector)\b", re.I)
VIA_EN_NOMBRE = re.compile(r"^(?:gran avenida|avenida|calle|pasaje|camino|autopista|carretera|costanera|diagonal|"
                           r"circunvalacion|plaza|ruta|caletera|villa|poblacion|sector)\b", re.I)
PATRONES_RESIDUALES = {  # patrones que aun no se resuelven: sirven para la siguiente iteracion
    "numero final sin marcador": re.compile(r"\s\d{2,5}$"),
    "contiene ' con '":          re.compile(r"\scon\s(?!con\b)", re.I),
    "contiene 'cruce'":          re.compile(r"\bcruce\b", re.I),
    "contiene 'frente/altura'":  re.compile(r"\b(?:frente|altura|interior|paradero|salida|entrada)\b", re.I),
    "contiene parentesis":       re.compile(r"[()]"),
    "contiene ' - '":            re.compile(r"\s-\s"),
    "contiene 'km' en calle":    re.compile(r"\bkm\b", re.I),
    "nombre generico":           re.compile(r"^(?:camino publico|camino vecinal|camino interior|calle|pasaje|avenida|camino)(?:\s\d+)?$", re.I),
    "texto muy corto (<4)":      re.compile(r"^.{1,3}$"),
    "texto muy largo (>60)":     re.compile(r"^.{61,}$"),
    "solo numeros":              re.compile(r"^\d+$"),
}


def _tok_titulo(t: str, primero: bool, previo: str = "") -> str:
    base = t.lower().rstrip(".")
    if ROMANO.match(base) and len(base) > 1:
        return base.upper()
    if not primero and base in PARTICULAS:
        return base
    if not primero and base in ARTICULOS and previo.lower() in PARTICULAS:
        return base
    if "'" in t:
        return "'".join(p[:1].upper() + p[1:].lower() for p in t.split("'"))
    if "-" in t:
        return "-".join(p[:1].upper() + p[1:].lower() for p in t.split("-"))
    return t[:1].upper() + t[1:].lower()


def es_fecha_excel(n) -> bool:
    return n is not None and str(n).isdigit() and FECHA_EXCEL[0] <= int(n) <= FECHA_EXCEL[1]


@functools.lru_cache(maxsize=None)
def limpiar_via(nombre):
    """
    Limpia un nombre de via. Devuelve (texto|None, reglas aplicadas, km encontrado en el texto).
    Las reglas se registran por nombre para el log de depuracion.
    """
    reglas: list[str] = []
    km = None
    if nombre is None or (not isinstance(nombre, str) and pd.isna(nombre)):
        return None, reglas, km
    t = re.sub(r"\s+", " ", str(nombre)).strip(" ,;.-|")
    t = re.sub(r"(?<=[A-Za-z])\"(?=[A-Za-z])", "'", t)                    # O"Higgins -> O'Higgins
    t = re.sub(r"(?i)\s*\bS/N\b\.?", "", t).strip(" ,;-")
    if t.lower() in VACIOS_DIR:
        return None, ["valor vacio o sin informacion"], km
    if RE_FECHA_TXT.match(t) or re.fullmatch(r"\d+(?:\.\d+)?", t):
        return None, ["fecha o numero en lugar de calle"], km
    m = RE_SUFIJO_TIPO.match(t)
    if m:
        nombre_v, tipo_v = m.group(1).strip(), m.group(2).strip()
        if tipo_v and not VIA_EN_NOMBRE.match(nombre_v.lower().replace("avda", "avenida")):
            t = f"{tipo_v} {nombre_v}"
        else:
            t = nombre_v
            if tipo_v:
                reglas.append("tipo de via ya incluido en el nombre")
        reglas.append("tipo de via al frente (=>)")
    m = RE_GUION_TIPO.match(t)
    if m:
        t = f"{m.group(2)} {m.group(1)}"
        reglas.append("tipo de via al frente (' - Tipo')")
    m = re.fullmatch(r"(\w+) - (\w+(?: \w+)*)", t)
    if m and m.group(1).lower() in TIPOS_VIA:
        t = f"{m.group(1)} {m.group(2)}"
        reglas.append("tipo de via al frente (' - Tipo')")
    elif m and " " not in m.group(2):
        t = t.replace(" - ", "-")
        reglas.append("nombre con guion unido (Colo-Colo)")
    if RE_PREFIJO_LUGAR.match(t) and RE_PREFIJO_LUGAR.sub("", t).strip():
        t = RE_PREFIJO_LUGAR.sub("", t).strip()
        reglas.append("se quita 'Frente/Interior/Altura' inicial")
    if RE_PARENTESIS.search(t):
        t2 = RE_PARENTESIS.sub(" ", t).strip(" ,;-")
        if t2:
            t = t2
            reglas.append("se quita texto entre parentesis")
    m = RE_SECTOR.match(t)
    if m and m.group(2).strip():
        base, lugar = m.group(1).strip(" ,;-."), m.group(2).strip(" ,;-.")
        if not base or base.lower() in GENERICOS:
            t = f"Sector {lugar}"
            reglas.append("camino generico: se usa el sector")
        else:
            t = base
            reglas.append("se quita 'Sector: ...'")
    mk = RE_KM_EN_TEXTO.search(t)
    if mk:
        km = mk.group(1)
        t2 = (t[:mk.start()] + t[mk.end():]).strip(" ,;-")
        t2 = RE_ALTURA.sub("", t2).strip(" ,;-")
        if t2 and not re.fullmatch(r"(?i)(?:a\s+la\s+)?altura|sector", t2):
            t = t2
        else:
            t = ""
        reglas.append("km dentro del nombre")
    t2 = RE_ALTURA.sub("", t).strip(" ,;-")                  # "Calle La Concepcion Frente" -> "Calle La Concepcion"
    if t2 and t2 != t:
        t = t2
        reglas.append("se quita 'Frente/Altura' final")
    t = re.sub(r"\.(?=\S)", ". ", t)                        # "Avda.Grecia" -> "Avda. Grecia"
    toks = [x for x in t.split(" ") if x]
    nuevos = []
    for i, tok in enumerate(toks):
        base = tok.lower().rstrip(".")
        if i == 0 and base in TIPOS_VIA:
            if TIPOS_VIA[base].lower() != base:
                reglas.append("abreviatura de tipo de via")
            nuevos.append(TIPOS_VIA[base])
        elif base == "av" and i > 0 and toks[i - 1].lower() == "gran":
            reglas.append("abreviatura de titulo o nombre")
            nuevos.append("Avenida")
        elif base in ABREVIATURAS:
            reglas.append("abreviatura de titulo o nombre")
            nuevos.append(ABREVIATURAS[base])
        else:
            nuevos.append(tok)
    t = " ".join(n for n in nuevos if n)
    t = re.sub(r"\s+", " ", t).strip(" ,;.-")
    if not t:
        return None, reglas, km
    toks = t.split(" ")
    inicio_nombre = 1 if toks and toks[0] in TIPOS_VIA.values() else 0
    t = " ".join(_tok_titulo(tok, i <= inicio_nombre and not (i == 1 and inicio_nombre == 1 and tok.lower() in ("a", "al")),
                             toks[i - 1] if i else "")
                 for i, tok in enumerate(toks))
    if t.lower() in VACIOS_DIR or sin_tipo(t) in VACIOS_DIR or not re.search(r"[A-Za-z]", t):
        return None, reglas + ["valor vacio o sin informacion"], km
    return t, reglas, km


@functools.lru_cache(maxsize=None)
def limpiar_ruta(r):
    """
    Ruta en forma estandar: 5, 68, E-71, CH-60. Devuelve (ruta|None, reglas, texto_via).
    texto_via trae el valor cuando no es un codigo de ruta (por ejemplo "Acceso Sur"), para
    usarlo como nombre de via.
    """
    reglas: list[str] = []
    if r is None or (not isinstance(r, str) and pd.isna(r)):
        return None, reglas, None
    t = re.sub(r"\s+", " ", str(r)).strip(" ,;.-")
    if t.lower() in SIN_ROL:
        return None, ["ruta sin rol (S/R)"], None
    partes = re.split(r"\s*[,/;]\s*|\s+y\s+", t)
    if len(partes) > 1 and all(partes):
        reglas.append("ruta multiple (se usa la primera)")
    t = re.sub(r"(?i)^((ruta|rta\.?|carretera|caletera)\s+)+", "", partes[0]).strip()
    m = RE_RUTA_COD.match(t)
    if m and (len(m.group(1)) == 2 or len(m.group(2)) >= 2 or m.group(1).lower() == "ch"):
        if t[m.end():].strip(" ,;-"):
            reglas.append("ruta con descripcion (se usa el codigo)")
        return f"{m.group(1).upper()}-{int(m.group(2))}", reglas + ["ruta con codigo"], None
    m = RE_RUTA_NUM.match(t)
    if m:
        return str(int(m.group(1))), reglas, None
    if RE_LONGITUDINAL.match(t):
        return "5", reglas + ["longitudinal o panamericana como Ruta 5"], None
    return None, reglas + (["ruta sin codigo: se usa como nombre de via"] if t else []), (t or None)


def ruta_en_calle(c1):
    """Codigo de ruta si la calle es en realidad una ruta; None en otro caso."""
    if not c1:
        return None
    if RE_LONGITUDINAL.match(c1):
        return "5"
    m = RE_RUTA_EN_CALLE.match(c1)
    if not m:
        return None
    g = m.groupdict()
    texto = g["a"] or g["b"] or g["g"] or (f"{g['c']}-{g['d']}" if g["c"] else f"{g['e']}-{g['f']}")
    return limpiar_ruta(texto)[0]


def limpiar_km(k):
    if k is None or (not isinstance(k, str) and pd.isna(k)):
        return None
    t = str(k).strip().lower().replace("km", "").replace(",", ".").strip(" .")
    try:
        v = float(t)
    except ValueError:
        return None
    if v < 0 or v > 3500:
        return None
    return str(int(v)) if v == int(v) else f"{v:.1f}"


def limpiar_numero(n):
    if n is None or (not isinstance(n, str) and pd.isna(n)):
        return None
    m = re.match(r"^\D*?(\d{1,5})\s*([A-Za-z])?\b", str(n).strip())
    if not m or int(m.group(1)) == 0:
        return None
    return str(int(m.group(1))) + (m.group(2).upper() if m.group(2) else "")


def sin_tipo(v: str) -> str:
    """Nombre de la via sin el tipo inicial, para comparar 'Avenida 5 de Abril' con '5 de Abril'."""
    toks = v.lower().split(" ")
    return " ".join(toks[1:]) if len(toks) > 1 and toks[0].title() in TIPOS_VIA.values() else v.lower()


def _partir_interseccion(c1):
    """Separa 'A Esquina B' o 'A con B' cuando ambos lados son nombres de via validos."""
    for patron, regla in ((RE_ESQUINA, "interseccion dentro de calle_1"), (RE_CON, "interseccion escrita con 'con'")):
        partes = patron.split(c1, maxsplit=1)
        if len(partes) != 2:
            continue
        izq, der = partes[0].strip(), re.sub(r"\s\d{1,5}$", "", partes[1]).strip()
        if izq.lower() in {v.lower() for v in TIPOS_VIA.values()} or len(sin_tipo(izq)) < 3 or len(sin_tipo(der)) < 3:
            continue
        a, b = limpiar_via(izq)[0], limpiar_via(der)[0]
        if a and b:
            return a, b, regla
    return None


def armar_direccion(c1_raw, c2_raw, r_raw, k_raw, f_raw, ur_raw=None, kamb_raw=None):
    """
    Direccion unica a partir de los componentes. Devuelve (direccion, tipo, reglas).
    kamb_raw marca los archivos sin columna de numero de frente (2025 en adelante), donde la
    columna KM trae tanto kilometros como numeros de casa.
    """
    reglas = []
    urbano = (ur_raw or "").lower().startswith("urb")
    km_ambiguo = bool(kamb_raw)
    # calle_2 que en realidad es un numero (o una fecha de Excel)
    if c2_raw is not None and re.fullmatch(r"\s*(?:N\u00b0\s*)?\d{1,5}\s*[A-Za-z]?\s*", str(c2_raw)):
        if es_fecha_excel(re.sub(r"\D", "", str(c2_raw))):
            reglas.append("numero con forma de fecha Excel (descartado)")
        else:
            f_raw = f_raw if limpiar_numero(f_raw) else c2_raw
            reglas.append("calle_2 era un numero")
        c2_raw = None
    c1, r1, km1 = limpiar_via(c1_raw)
    c2, r2, km2 = limpiar_via(c2_raw)
    ruta, rr, via_ruta = limpiar_ruta(r_raw)
    reglas += r1 + r2 + rr
    if via_ruta:
        v = limpiar_via(via_ruta)[0]
        if v and not c1:
            c1 = v
        elif v and c1 and not c2 and sin_tipo(v) != sin_tipo(c1):
            c2 = v
    km = limpiar_km(k_raw)
    km_de_texto = False
    if not km and (limpiar_km(km1) or limpiar_km(km2)):
        km = limpiar_km(km1) or limpiar_km(km2)
        km_de_texto = True
        reglas.append("km tomado del nombre de la via")
    km_grande = None   # km mayor que 3500: metros en rutas, o numero de casa en 2025+
    if km is None and k_raw is not None:
        try:
            _v = float(str(k_raw).replace(",", "."))
            if 3500 < _v <= 3_500_000:
                km_grande = _v
        except ValueError:
            pass
    num = limpiar_numero(f_raw)
    if es_fecha_excel(num):
        num = None
        reglas.append("numero con forma de fecha Excel (descartado)")

    # Numero escrito dentro de la calle con marcador (N°, Nro, #): prevalece sobre calle_2
    if c1:
        m = RE_NUM_MARCADO.search(c1)
        if m:
            num, c1 = limpiar_numero(m.group(1)), RE_ALTURA.sub("", c1[:m.start()]).strip(" ,")
            reglas.append("numero dentro de la calle (con marcador)")
            if c2:
                c2 = None
                reglas.append("numero explicito prevalece sobre calle_2")
    # calle_2 que describe un lugar (estacionamiento, mall, servicentro) y no una calle
    if c1 and c2 and POI_PALABRAS.search(c2) and not re.match(r"(?i)^(?:avenida|calle|pasaje|camino)\s", c2):
        c2 = None
        reglas.append("calle_2 describe un lugar (se descarta)")
    # Interseccion escrita dentro de calle_1
    if c1 and not c2:
        p = _partir_interseccion(c1)
        if p:
            c1, c2, regla = p
            reglas.append(regla)
    if c1 and c2 and sin_tipo(c1) == sin_tipo(c2):
        c1, c2 = (c1 if len(c1) >= len(c2) else c2), None
        reglas.append("calle_1 igual a calle_2")
    if not c1 and c2:
        c1, c2 = c2, None
    # calle_2 que es una ruta: se escribe en forma estandar ("Carretera Ruta 68" -> "Ruta 68")
    if c2:
        rc2 = ruta_en_calle(c2)
        if rc2:
            c2 = f"Ruta {rc2}"
            reglas.append("ruta escrita en calle_2")
    # Calle que es una ruta (Ruta 68, Carretera Ruta 68, Longitudinal Sur) sin ruta informada
    if c1 and not ruta:
        rc = ruta_en_calle(c1)
        es_longitudinal = bool(RE_LONGITUDINAL.match(c1))
        if rc:
            if urbano and num and not km and es_longitudinal and int(re.sub(r"\D", "", num) or 0) >= 100:
                reglas.append("via urbana con nombre de ruta y numero (se mantiene la calle)")
            elif c2 and not km:
                c1 = f"Ruta {rc}"
                reglas.append("ruta escrita en la calle (interseccion)")
            else:
                ruta = rc
                reglas.append("ruta escrita en la calle")
                c1, c2 = None, None
    if km_grande is not None:
        if (km_ambiguo and urbano and not ruta and c1 and not c2 and not num and not VIA_RURAL.match(c1)
                and km_grande.is_integer() and km_grande < 100_000):
            num = str(int(km_grande))
            reglas.append("km interpretado como numero (via urbana)")
        else:
            km = limpiar_km(km_grande / 1000)
            reglas.append("km en metros (se divide por 1000)")
    # "Cruce Longitudinal - X" con km en zona rural: es la Ruta 5
    if c1 and not ruta and km and not urbano and re.search(r"\b(?:longitudinal|panamericana)\b", c1, re.I):
        ruta, c1, c2 = "5", None, None
        reglas.append("longitudinal dentro del nombre con km (Ruta 5)")
    # Numero de frente usado como km en una ruta (siempre en zona rural; en zona urbana si es menor que 1000)
    if ruta and not km and num and (not urbano or int(re.sub(r"\D", "", num) or 0) < 1000):
        km, num = limpiar_km(num), None
        reglas.append("numero de frente usado como km (ruta rural)")
    # Desde 2025 la columna KM trae tambien el numero de casa en vias urbanas
    if (km_ambiguo and not km_de_texto and not ruta and km and not num and c1 and not c2 and urbano
            and not VIA_RURAL.match(c1) and "." not in km):
        num, km = km, None
        reglas.append("km interpretado como numero (via urbana)")
    # Orden alfabetico de la interseccion: A & B y B & A son la misma esquina (una sola consulta)
    if c1 and c2 and sin_tipo(c2) < sin_tipo(c1):
        c1, c2 = c2, c1
        reglas.append("interseccion en orden alfabetico")

    # Un sector es una localidad: no lleva numero de casa
    if c1 and not c2 and num and c1.startswith("Sector "):
        num = None
        reglas.append("sector sin numero (se descarta el numero)")

    if ruta and km:
        return FMT_RUTA_KM.format(r=ruta, k=km), "ruta_km", reglas, (None, None, None, ruta, km)
    if c1 and c2:
        return FMT_INTERSECCION.format(c1=c1, c2=c2), "interseccion", reglas, (c1, c2, None, None, None)
    if c1 and num:
        return FMT_NUMERO.format(c1=c1, n=num), "numero", reglas, (c1, None, num, None, None)
    if c1 and km:
        return FMT_VIA_KM.format(c1=c1, k=km), "via_km", reglas, (c1, None, None, None, km)
    if ruta:
        return FMT_RUTA.format(r=ruta), "ruta", reglas, (None, None, None, ruta, None)
    if c1:
        return c1, "solo_calle", reglas, (c1, None, None, None, None)
    if num:
        reglas.append("numero sin calle (descartado)")
    return None, "sin_dato", reglas, (None, None, None, None, None)


if DESDE <= 7 <= HASTA:
    _t0 = time.time()
    log.info("=== ETAPA 7: direccion unica (log de depuracion en %s) ===", DIR_LOG_PATH)
    dlog.info("=== ETAPA 7: direccion unica para geolocalizar ===")
    _ok = _fa = 0
    try:
        _kc = ["calle_1", "calle_2", "ruta", "km", "frente_nro", "urbano_rural", "km_ambiguo"]   # componentes
        _cols = ["anio_archivo", "id_accidente", "cod_comuna"] + _kc
        _norm = [p for p, g, _, _ in listar_archivos(NORM_DIR, ("csv",)) if g == "accidentes"]
        _partes_c = []
        for _p in _norm:
            _d = leer_csv(_p)[[c for c in _cols if c != "km_ambiguo"]]
            # Archivos sin numero de frente (2025 en adelante): la columna KM mezcla km y numeros de casa
            _d["km_ambiguo"] = "1" if _d["frente_nro"].isna().all() else pd.NA
            _partes_c.append(_d)
        _comp = pd.concat(_partes_c, ignore_index=True)
        dlog.info("Archivos con KM ambiguo (sin numero de frente): %s",
                  [p.name for p, d in zip(_norm, _partes_c) if d["km_ambiguo"].notna().any()])
        dlog.info("Componentes leidos: %s filas desde %d archivos de %s", miles(len(_comp)), len(_norm), NORM_DIR)
        for _c in _kc[:5]:
            dlog.info("  %-10s %5.1f %% completo", _c, _comp[_c].notna().mean() * 100)

        # Se procesa cada combinacion distinta de componentes una sola vez
        _comb = _comp[_kc].fillna(SENT)
        _unicos = _comb.value_counts().reset_index(name="n")
        dlog.info("Combinaciones distintas de componentes: %s", miles(len(_unicos)))
        _res = [armar_direccion(*[None if v == SENT else str(v) for v in fila])
                for fila in _unicos[_kc].itertuples(index=False, name=None)]
        _unicos["direccion"] = [r[0] for r in _res]
        _unicos["tipo_direccion"] = [r[1] for r in _res]
        _unicos["reglas"] = [" | ".join(dict.fromkeys(r[2])) for r in _res]
        for _i, _c in enumerate(COMPONENTES_DIR):
            _unicos[_c] = [r[3][_i] for r in _res]
        _comp = _comb.merge(_unicos, on=_kc, how="left").assign(
            anio_archivo=_comp["anio_archivo"].values, id_accidente=_comp["id_accidente"].values,
            cod_comuna=_comp["cod_comuna"].values)

        # --- Diagnostico 1: tipo de direccion por anio
        _tab = pd.crosstab(_comp["anio_archivo"], _comp["tipo_direccion"], normalize="index").mul(100).round(1)
        dlog.info("Tipo de direccion por anio (%% de filas):\n%s", _tab.to_string())
        _tot = _comp["tipo_direccion"].value_counts()
        dlog.info("Tipo de direccion total: %s", " | ".join(f"{k}: {miles(v)} ({v / len(_comp) * 100:.1f} %)" for k, v in _tot.items()))

        # --- Diagnostico 2: filas tocadas por cada regla, con ejemplos antes y despues
        _ex = _unicos.assign(regla=_unicos["reglas"].str.split(r" \| ")).explode("regla")
        _ex = _ex[_ex["regla"].fillna("") != ""]
        _por_regla = _ex.groupby("regla")["n"].sum().sort_values(ascending=False)
        dlog.info("Filas tocadas por regla:\n%s", "\n".join(f"  {miles(v):>10}  {k}" for k, v in _por_regla.items()))
        _aud = []
        for _regla in _por_regla.index:
            _m = _ex[_ex["regla"] == _regla].sort_values("n", ascending=False).head(25)
            _aud.append(_m)
            dlog.info("  Ejemplos '%s':", _regla)
            for _f in _m.head(5).itertuples(index=False):
                _antes = " | ".join(str(v) for v in (_f.calle_1, _f.calle_2, _f.ruta, _f.km, _f.frente_nro) if v != SENT)
                _zona = "" if _f.urbano_rural == SENT else f"({_f.urbano_rural}) "
                dlog.info("    [%s] %s%s  ->  %s", miles(_f.n), _zona, _antes, _f.direccion)
        if _aud:
            pd.concat(_aud).replace(SENT, "").to_csv(DIR_DIR / "auditoria_reglas.csv", index=False, sep=CSV_SEP, encoding=ENCODING)

        # --- Diagnostico 3: posibles abreviaturas no reconocidas (palabras cortas frecuentes)
        _vias = pd.concat([_comb["calle_1"], _comb["calle_2"]])
        _vias = _vias[_vias != SENT]
        _palabras = _vias.str.split().explode().str.lower().str.rstrip(".")
        _cortas = _palabras[_palabras.str.fullmatch(r"[a-z]{2,5}") & ~_palabras.isin(set(TIPOS_VIA) | set(ABREVIATURAS) | PARTICULAS)]
        _vc = _cortas.value_counts()
        _sin_vocal = _vc[_vc.index.str.fullmatch(r"[^aeiou]+")].head(40)
        dlog.info("Palabras sin vocales (probables abreviaturas no reconocidas):\n%s",
                  "\n".join(f"  {miles(v):>9}  {k}" for k, v in _sin_vocal.items()) or "  (ninguna)")
        _primeras = _vias.str.split().str[0].str.lower().str.rstrip(".").value_counts().head(40)
        dlog.info("Primera palabra mas frecuente de las vias (para detectar tipos de via):\n%s",
                  "\n".join(f"  {miles(v):>9}  {k}{'  (reconocido)' if k in TIPOS_VIA else ''}" for k, v in _primeras.items()))
        _vc.head(300).rename_axis("palabra").reset_index(name="filas").to_csv(
            DIR_DIR / "palabras_cortas.csv", index=False, sep=CSV_SEP, encoding=ENCODING)

        # --- Diagnostico 4: patrones residuales en la direccion final
        _fin = _unicos[_unicos["direccion"].notna()]
        _sin_km = ~_fin["tipo_direccion"].isin(["ruta_km", "via_km", "ruta"])
        _filas_res = []
        dlog.info("Patrones residuales en la direccion final (filas | ejemplos):")
        for _nombre, _pat in PATRONES_RESIDUALES.items():
            _m = _fin[_fin["direccion"].str.contains(_pat, na=False)]
            if _nombre == "numero final sin marcador":
                _m = _m[_m["tipo_direccion"].isin(["solo_calle", "interseccion"])]
            elif _nombre == "contiene 'km' en calle":
                _m = _m[_sin_km.loc[_m.index]]
            if len(_m):
                _ej = _m.sort_values("n", ascending=False).head(4)["direccion"].tolist()
                dlog.info("  %10s  %-28s %s", miles(int(_m["n"].sum())), _nombre, " | ".join(_ej))
                _filas_res.append(_m.sort_values("n", ascending=False).head(50).assign(patron=_nombre))
        if _filas_res:
            pd.concat(_filas_res).replace(SENT, "").to_csv(DIR_DIR / "patrones_residuales.csv", index=False, sep=CSV_SEP, encoding=ENCODING)

        # --- Diagnostico 5: direcciones finales mas repetidas (suelen delatar basura o genericos)
        _top = _comp["direccion"].value_counts().head(30)
        dlog.info("Direcciones mas repetidas:\n%s", "\n".join(f"  {miles(v):>8}  {k}" for k, v in _top.items()))

        # --- Diagnostico 6: muestras aleatorias para verificar la escritura de los nombres
        def _linea(f) -> str:
            comps = " | ".join(str(getattr(f, c)) for c in ["calle_1", "calle_2", "ruta", "km", "frente_nro"]
                               if getattr(f, c) != SENT)
            cut = str(f.cod_comuna).zfill(5) if pd.notna(f.cod_comuna) else ""
            com = TABLA_CUT[cut][0] if cut in TABLA_CUT else "?"
            zona = "" if f.urbano_rural == SENT else f.urbano_rural
            reglas = (f.reglas or "")[:110] if isinstance(f.reglas, str) else ""
            dir_ = f.direccion if isinstance(f.direccion, str) else "<sin direccion>"
            return f"  [{f.anio_archivo} | {com} | {zona}] {comps or '<vacio>'}  ->  {dir_}" + (f"   ({reglas})" if reglas else "")

        _rs = SEMILLA_MUESTRA
        _muestras = []
        dlog.info("MUESTRAS ALEATORIAS (semilla %d; con --semilla %d se repiten)", _rs, _rs)
        for _tipo, _g in _comp.groupby("tipo_direccion"):
            _m = _g.sample(n=min(8, len(_g)), random_state=_rs)
            _muestras.append(_m.assign(muestra=f"tipo: {_tipo}"))
            dlog.info("Muestra aleatoria, tipo '%s' (%s filas de ese tipo):", _tipo, miles(len(_g)))
            for _f in _m.itertuples(index=False):
                dlog.info(_linea(_f))
        _mod = _comp[_comp["reglas"].fillna("").str.contains(r"abreviatura|tipo de via|via al frente|guion|parentesis", regex=True)]
        if len(_mod):
            _m = _mod.sample(n=min(20, len(_mod)), random_state=_rs)
            _muestras.append(_m.assign(muestra="nombres modificados"))
            dlog.info("Muestra aleatoria de nombres modificados (abreviaturas, tipo de via, guiones, parentesis; %s filas):",
                      miles(len(_mod)))
            for _f in _m.itertuples(index=False):
                dlog.info(_linea(_f))
        _m = pd.concat([_g.sample(n=min(2, len(_g)), random_state=_rs) for _, _g in _comp.groupby("anio_archivo")])
        _muestras.append(_m.assign(muestra="por anio"))
        dlog.info("Muestra aleatoria por anio (2 por anio):")
        for _f in _m.itertuples(index=False):
            dlog.info(_linea(_f))
        pd.concat(_muestras).replace(SENT, "").to_csv(DIR_DIR / "muestra_aleatoria.csv", index=False, sep=CSV_SEP, encoding=ENCODING)

        # --- Diagnostico 7: consultas distintas que habria que geocodificar (direccion + comuna)
        _q = _comp.dropna(subset=["direccion"]).drop_duplicates(["direccion", "cod_comuna"])
        dlog.info("Consultas distintas para geocodificar (direccion + comuna): %s de %s filas con direccion (%.1f %%)",
                  miles(len(_q)), miles(int(_comp["direccion"].notna().sum())),
                  len(_q) / max(1, _comp["direccion"].notna().sum()) * 100)
        dlog.info("  por tipo: %s", " | ".join(f"{k}: {miles(v)}" for k, v in _q["tipo_direccion"].value_counts().items()))

        # --- Escritura: direccion reemplaza a ubicacion en el consolidado de siniestros
        _ruta_sin = UNION_DIR / "siniestros.csv"
        if _ruta_sin.exists():
            _sin = leer_csv(_ruta_sin)
            _sin = _sin.drop(columns=["ubicacion", "direccion"], errors="ignore").merge(
                _comp[["anio_archivo", "id_accidente", "direccion"]], on=["anio_archivo", "id_accidente"], how="left")
            _pos = list(_sin.columns).index("causa") + 1 if "causa" in _sin.columns else len(_sin.columns) - 1
            _sin.insert(_pos, "direccion", _sin.pop("direccion"))
            escribir_csv_seguro(_sin, _ruta_sin)
            log.info("Escrito: union\\siniestros.csv con columna direccion (%.1f %% con dato)", _sin["direccion"].notna().mean() * 100)
            dlog.info("Escrito: %s (%s filas, %.1f %% con direccion)", _ruta_sin, miles(len(_sin)), _sin["direccion"].notna().mean() * 100)
        else:
            dlog.warning("No existe %s: corre antes la etapa 6.", _ruta_sin)
        _comp[["anio_archivo", "id_accidente", "cod_comuna", "direccion", "tipo_direccion", "reglas"] + COMPONENTES_DIR].to_csv(
            DIR_DIR / "direcciones.csv", index=False, sep=CSV_SEP, encoding=ENCODING)
        dlog.info("Archivos de depuracion en %s: direcciones.csv, auditoria_reglas.csv, palabras_cortas.csv, "
                  "patrones_residuales.csv, muestra_aleatoria.csv", DIR_DIR)
        _ok = 1
    except Exception as exc:
        log.error("Fallo en la etapa 7: %s: %s", type(exc).__name__, exc)
        dlog.exception("Fallo en la etapa 7")
        _fa = 1
    dlog.info("Etapa 7 terminada en %s", duracion(time.time() - _t0))
    RESUMEN_ETAPAS[7] = {"nombre": "direcciones", "ok": _ok, "omitidos": 0, "fallidos": _fa, "seg": time.time() - _t0}

# ===========================================================================
# ETAPA 8: coordenadas con OpenStreetMap (con log propio de depuracion)
# ===========================================================================
# Datos de OpenStreetMap (c) colaboradores de OpenStreetMap, licencia ODbL.
# Si se publican las coordenadas hay que citar la fuente: https://www.openstreetmap.org/copyright
OSM_URL      = "https://download.geofabrik.de/south-america/chile-latest.osm.pbf"
OSM_DIR      = DATA_DIR / "osm"
OSM_PBF      = OSM_DIR / "chile-latest.osm.pbf"
OSM_INDICE   = OSM_DIR / "indice_osm.pkl"
OSM_INDICE_VERSION = 7                # cambia cuando cambia la estructura del indice
OSM_MAX_DIAS = 30                     # se vuelve a descargar si el extracto tiene mas dias que esto
OSM_ACTUALIZAR = "--osm-actualizar" in ARGS
GEO_DIR      = DATA_DIR / "geolocalizacion"
# Red vial de la Direccion de Vialidad (MOP): cada tramo trae su medida M en metros desde el inicio del rol,
# lo que permite ubicar "ruta X, km Y". Capa publica en ArcGIS Online; geometria 1:350.000 (actualizacion 2013).
MOP_URL      = "https://services3.arcgis.com/Dhl01RVOOnbjTdY7/ArcGIS/rest/services/RED_VIAL_MOP/FeatureServer/0/query"
MOP_DIR      = DATA_DIR / "mop"
MOP_DATOS    = MOP_DIR / "red_vial_mop.pkl"
MOP_INDICE   = MOP_DIR / "indice_mop.pkl"
MOP_MAX_DIAS = 180
GEO_LOG_PATH = DATA_DIR / "geolocalizacion.log"
# Alias de calles mantenidos a mano (no se regenera). Columnas: cod_comuna (vacio = todas), nombre_dato,
# nombre_osm, lat, lon, precision_m, nota. nombre_osm = "-" bloquea el emparejamiento (para falsos positivos);
# lat/lon dan un punto para calles que no existen en OSM. El archivo alias_sugeridos.csv (en geolocalizacion/)
# se regenera en cada corrida con las calles sin pareja y hasta 3 sugerencias; las filas que se completen ahi
# (nombre_osm o lat/lon) se copian solas a este archivo en la corrida siguiente.
ALIAS_MANUAL = DATA_DIR / "alias_calles.csv"
COLS_ALIAS = ["cod_comuna", "comuna", "nombre_dato", "filas", "nombre_osm", "lat", "lon", "precision_m", "nota"]
BLOQUEO = "__bloqueado__"
UMBRAL_NOMBRE = 88                    # similitud minima (0-100) para aceptar un nombre de calle aproximado
CRUCE_MAX_M   = 40                    # distancia maxima entre dos calles para aceptar un cruce sin nodo comun
NUM_MAX_DIF   = 200                   # diferencia maxima de numeracion para interpolar
VIALES_OSM = {"motorway", "trunk", "primary", "secondary", "tertiary", "unclassified", "residential",
              "living_street", "service", "road", "motorway_link", "trunk_link", "primary_link",
              "secondary_link", "tertiary_link", "track", "pedestrian"}
PLACES_OSM = {"village", "hamlet", "locality", "isolated_dwelling", "neighbourhood", "suburb", "quarter", "farm", "town"}
# Precision aproximada (metros) de cada metodo, para filtrar en el analisis
PRECISION_M = {
    "punto manual": 100,
    "cruce exacto": 10, "cruce exacto (variante de nombre)": 25, "cruce exacto (nombre alternativo)": 25, "cruce por cercania": 40, "numero exacto": 15, "numero interpolado": 60,
    "numero cercano": 150, "numero interpolado (tramo largo)": 150, "km sobre red vial MOP": 300, "km entre hitos": 300, "km en hito": 100, "calle en la comuna": 1000,
    "ruta en la comuna": 5000, "localidad": 1500,
}
ALIAS_OSM = {  # clave de la direccion -> clave del nombre en OSM (se prueba solo si la clave original no existe)
    "alameda": ("libertador bernardo ohiggins", {"13", "06"}),
    "alameda libertador bernardo ohiggins": "libertador bernardo ohiggins",
    "alameda bernardo ohiggins": "libertador bernardo ohiggins",
    "bernardo ohiggins": "libertador bernardo ohiggins",
    "11 de septiembre": "nueva providencia",          # Providencia: renombrada en 2013
    "costanera monsenor escriva de balaguer": "san josemaria escriva de balaguer",   # Vitacura
    "monsenor escriva de balaguer": "san josemaria escriva de balaguer",
    # alias propios de una zona: (destino, regiones donde aplican)
    "vespucio": ("americo vespucio", {"13"}),
    "gran avenida": ("jose miguel carrera", {"13"}),
    "panamericana norte": "panamericana norte", "costanera norte": "costanera norte",
}
def alias_osm(k: str, cut: str):
    """Alias de la clave para la comuna; los alias con regiones solo aplican dentro de ellas."""
    a = ALIAS_OSM.get(k)
    if isinstance(a, tuple):
        return a[0] if str(cut)[:2] in a[1] else None
    return a


RE_TIPO_OSM = re.compile(r"^(?:gran avenida|avenida|av|avda|calle|pasaje|psje|camino|autopista|carretera|costanera|"
                         r"diagonal|circunvalacion|paseo|sector|villa|poblacion)\s+")

glog = logging.getLogger("geolocalizacion")
glog.setLevel(logging.INFO)
glog.propagate = False


NUMEROS_PALABRA = {
    "uno": "1", "una": "1", "dos": "2", "tres": "3", "cuatro": "4", "cinco": "5", "seis": "6", "siete": "7",
    "ocho": "8", "nueve": "9", "diez": "10", "once": "11", "doce": "12", "trece": "13", "catorce": "14",
    "quince": "15", "dieciseis": "16", "diecisiete": "17", "dieciocho": "18", "diecinueve": "19", "veinte": "20",
    "veintiuno": "21", "veintidos": "22", "veintitres": "23", "veinticuatro": "24", "veinticinco": "25",
    "treinta": "30",
}
# Palabras que no distinguen una calle de otra: se quitan para la clave "nucleo"
PALABRAS_VACIAS = {"de", "del", "la", "las", "los", "el", "y", "e", "a", "al", "en",
                   "general", "presidente", "doctor", "doctora", "alcalde", "monsenor", "senador", "cardenal",
                   "profesor", "coronel", "capitan", "teniente", "almirante", "comandante", "sargento", "padre",
                   "santa", "santo", "san", "caletera", "pasaje", "avenida", "calle", "camino", "ingeniero",
                   "arquitecto", "obispo", "oficial", "bombero", "mariscal", "intendente", "diputado", "diputada", "dip"}
# Palabras de 4 letras o mas demasiado comunes para identificar una calle por si solas
GENERICAS = {"chile", "nueva", "nuevo", "central", "principal", "norte", "oriente", "poniente", "rural", "interior",
             "publico", "vecinal", "union", "patria", "republica", "rotonda"}
TITULOS = PALABRAS_VACIAS - {"de", "del", "la", "las", "los", "el", "y", "e", "a", "al", "en"}
DIRECCIONALES = {"norte", "sur", "oriente", "poniente"}


def clave_osm(nombre) -> str:
    """
    Clave de comparacion de nombres de calle: minusculas, sin tildes ni signos, sin tipo de via inicial
    y con los numeros escritos en palabras pasados a cifras ("Dos Norte" y "2 Norte" dan la misma clave).
    """
    if nombre is None or (not isinstance(nombre, str) and pd.isna(nombre)):
        return ""
    s = str(nombre).replace("\u00bd", " 1/2").replace("\u00bc", " 1/4").replace("\u00be", " 3/4")   # 16½ Norte
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = " ".join(NUMEROS_PALABRA.get(t, t) for t in s.split())
    s = re.sub(r"\bo (?=[a-z]{3,})", "o", s)          # O'Higgins / O Carrol / OCarrol -> ohiggins / ocarrol
    # se quitan los tipos de via iniciales, aunque vengan dos ("avenida diagonal oriente" -> "oriente")
    for _ in range(2):
        s2 = RE_TIPO_OSM.sub("", s)
        if not s2 or s2 == s:
            break
        s = s2
    return s


def clave_nucleo(k: str) -> str:
    """Clave sin titulos ni particulas: 'presidente salvador allende' -> 'salvador allende'."""
    t = [x for x in k.split() if x not in PALABRAS_VACIAS]
    return " ".join(t) if t else k


def metros(lon1, lat1, lon2, lat2) -> float:
    """Distancia aproximada en metros (equirectangular, suficiente a escala urbana)."""
    import math
    x = (lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
    return math.hypot(x, lat2 - lat1) * 111_320


def descargar_osm() -> None:
    OSM_DIR.mkdir(parents=True, exist_ok=True)
    edad = (time.time() - OSM_PBF.stat().st_mtime) / 86400 if OSM_PBF.exists() else None
    if edad is not None and edad <= OSM_MAX_DIAS and not OSM_ACTUALIZAR:
        glog.info("Extracto OSM vigente (%.0f dias): %s", edad, OSM_PBF)
        return
    glog.info("Descargando extracto OSM de Chile: %s", OSM_URL)
    tmp = OSM_PBF.with_suffix(".tmp")
    with requests.get(OSM_URL, stream=True, timeout=TIMEOUT_SEC) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for bloque in r.iter_content(chunk_size=1 << 20):
                f.write(bloque)
    os.replace(tmp, OSM_PBF)
    glog.info("Descargado: %s (%.0f MB)", OSM_PBF.name, OSM_PBF.stat().st_size / 1024 / 1024)


def ruta_osmium(p: Path) -> str:
    """
    libosmium en Windows no abre rutas con caracteres no ASCII (por ejemplo 'Área' en la carpeta de
    OneDrive). Devuelve una ruta utilizable: la misma si es ASCII, la ruta corta de Windows (8.3) si
    existe, o un enlace duro o copia del archivo en una carpeta temporal con ruta ASCII.
    """
    s = str(p)
    if s.isascii():
        return s
    if os.name == "nt":
        try:
            import ctypes
            buf = ctypes.create_unicode_buffer(32768)
            # Solo la carpeta en forma corta: el nombre del archivo se conserva (ya es ASCII)
            n = ctypes.windll.kernel32.GetShortPathNameW(str(p.parent), buf, len(buf))
            if 0 < n < len(buf) and buf.value.isascii() and p.name.isascii():
                corta = buf.value.rstrip("\\") + "\\" + p.name
                glog.info("Ruta con caracteres no ASCII: osmium usara la ruta corta %s", corta)
                return corta
        except Exception:
            pass
    candidatas = [Path(tempfile.gettempdir()) / "siniestros_osm"]
    if os.name == "nt":
        candidatas.append(Path(os.environ.get("SystemDrive", "C:") + "\\") / "siniestros_osm")
    for d in candidatas:
        if not str(d).isascii():
            continue
        try:
            d.mkdir(parents=True, exist_ok=True)
        except OSError:
            continue
        dst = d / p.name
        if not (dst.exists() and dst.stat().st_size == p.stat().st_size and dst.stat().st_mtime >= p.stat().st_mtime):
            if dst.exists():
                dst.unlink()
            try:
                os.link(p, dst)          # enlace duro: no ocupa espacio extra en el mismo disco
                modo = "un enlace duro"
            except OSError:
                shutil.copy2(p, dst)
                modo = "una copia"
            glog.info("Ruta con caracteres no ASCII: osmium leera %s en %s", modo, dst)
        return str(dst)
    raise RuntimeError("No hay una carpeta con ruta ASCII para que osmium lea el extracto. "
                       "Mueve el proyecto a una ruta sin tildes ni enies.")


def construir_indice() -> dict:
    """
    Lee el extracto una sola vez y arma, por comuna (CUT): nombres de calle con sus geometrias,
    cruces entre calles, numeros de casa, rutas por codigo, hitos kilometricos y localidades.
    """
    import osmium
    import shapely
    from shapely.geometry import Point, LineString
    from shapely.strtree import STRtree
    import shapely.wkb as wkblib

    t0 = time.time()
    fab = osmium.geom.WKBFactory()

    class Lector(osmium.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.vias, self.direcciones, self.hitos, self.lugares, self.comunas = [], [], {}, [], []

        def node(self, n):
            t = n.tags
            if "addr:housenumber" in t and "addr:street" in t:
                m = re.match(r"\d+", t["addr:housenumber"])
                if m:
                    self.direcciones.append((t["addr:street"], int(m.group()), n.location.lon, n.location.lat))
            if t.get("highway") == "milestone":
                d = t.get("distance") or t.get("pk")
                try:
                    self.hitos[n.id] = [float(str(d).replace(",", ".").split()[0]), n.location.lon, n.location.lat, None]
                except (TypeError, ValueError):
                    pass
            if t.get("place") in PLACES_OSM and "name" in t:
                self.lugares.append((t["name"], n.location.lon, n.location.lat))

        def way(self, w):
            t = w.tags
            hw = t.get("highway")
            try:
                if hw in VIALES_OSM and ("name" in t or "ref" in t):
                    pts = [(n.ref, n.lon, n.lat) for n in w.nodes]
                    self.vias.append((t.get("name"), t.get("ref"), pts))
                    if "ref" in t and self.hitos:
                        for nid, _, _ in pts:
                            if nid in self.hitos and self.hitos[nid][3] is None:
                                self.hitos[nid][3] = t["ref"]
                elif "addr:housenumber" in t and "addr:street" in t:
                    m = re.match(r"\d+", t["addr:housenumber"])
                    if m and len(w.nodes):
                        nd = w.nodes[0]
                        self.direcciones.append((t["addr:street"], int(m.group()), nd.lon, nd.lat))
            except osmium.InvalidLocationError:
                pass

    class LectorComunas(osmium.SimpleHandler):
        """Segunda pasada solo para los limites comunales (armar poligonos es costoso)."""
        def __init__(self):
            super().__init__()
            self.comunas = []

        def area(self, a):
            t = a.tags
            if t.get("boundary") == "administrative" and t.get("admin_level") == "8":
                try:
                    g = wkblib.loads(fab.create_multipolygon(a), hex=True)
                except Exception:
                    return
                self.comunas.append((t.get("dpachile:id"), t.get("name"), g))

    pbf = ruta_osmium(OSM_PBF)
    lec = Lector()
    # El filtro descarta en C++ los objetos sin estas etiquetas (casi todos los nodos), sin perder ubicaciones
    # El formato se indica explicito: libosmium lo deduce de la extension y una ruta corta lo confunde
    lec.apply_file(osmium.io.File(pbf, "pbf"), locations=True, idx="flex_mem",
                   filters=[osmium.filter.KeyFilter("addr:housenumber", "highway", "place")])
    glog.info("  pasada 1 (calles, numeros, hitos, localidades): %s", duracion(time.time() - t0))
    lc = LectorComunas()
    lc.apply_file(osmium.io.File(pbf, "pbf"), locations=True, idx="flex_mem")
    lec.comunas = lc.comunas
    glog.info("  pasada 2 (limites comunales): %s", duracion(time.time() - t0))
    glog.info("Lectura OSM: %s vias con nombre o codigo, %s numeros de casa, %s hitos, %s localidades, %s comunas (%s)",
              miles(len(lec.vias)), miles(len(lec.direcciones)), miles(len(lec.hitos)), miles(len(lec.lugares)),
              len(lec.comunas), duracion(time.time() - t0))

    # Comunas OSM -> CUT (por dpachile:id; si falta, por nombre)
    poligonos, cuts = [], []
    for dpa, nombre, g in lec.comunas:
        cut = None
        if dpa and re.sub(r"\D", "", dpa).zfill(5) in TABLA_CUT:
            cut = re.sub(r"\D", "", dpa).zfill(5)
        elif nombre:
            k = clave_territorio(nombre)
            cut = NOMBRE_A_CUT.get(k) or NOMBRE_A_CUT.get(ALIAS_COMUNAS.get(k, ""))
        if cut:
            poligonos.append(g)
            cuts.append(cut)
    faltan = sorted(set(TABLA_CUT) - set(cuts))
    glog.info("Comunas OSM asociadas a un CUT: %d de %d. Sin poligono en OSM: %s", len(set(cuts)), len(TABLA_CUT),
              [f"{c} {TABLA_CUT[c][0]}" for c in faltan][:20])
    arbol = STRtree(poligonos)
    vecinas = {c: set() for c in set(cuts)}
    for i, j in zip(*arbol.query(poligonos, predicate="intersects")):
        if cuts[i] != cuts[j]:
            vecinas[cuts[i]].add(cuts[j])

    def comuna_de(lons, lats):
        """CUT de cada punto (dentro del poligono o, si cae justo fuera, el mas cercano a menos de ~1 km)."""
        pts = shapely.points(lons, lats)
        res = [None] * len(pts)
        ip, ig = arbol.query(pts, predicate="within")
        for a, b in zip(ip, ig):
            res[a] = cuts[b]
        faltan_i = [i for i, v in enumerate(res) if v is None]
        if faltan_i:
            ip, ig = arbol.query_nearest(pts[faltan_i], max_distance=0.01)
            for a, b in zip(ip, ig):
                if res[faltan_i[a]] is None:
                    res[faltan_i[a]] = cuts[b]
        return res

    # Vias: nombre -> geometrias por comuna (primer, medio y ultimo nodo definen las comunas de cada via)
    rep_lon, rep_lat, rep_via = [], [], []
    for iv, (nm, ref, pts) in enumerate(lec.vias):
        for k in {0, len(pts) // 2, len(pts) - 1}:
            rep_lon.append(pts[k][1]); rep_lat.append(pts[k][2]); rep_via.append(iv)
    com_rep = comuna_de(rep_lon, rep_lat)
    comunas_via = collections.defaultdict(set)
    for iv, c in zip(rep_via, com_rep):
        if c:
            comunas_via[iv].add(c)

    calles = collections.defaultdict(lambda: collections.defaultdict(list))   # cut -> clave -> [coords]
    nombre_osm = collections.defaultdict(collections.Counter)                 # clave -> nombres originales
    nombre_osm_c = collections.defaultdict(lambda: collections.defaultdict(collections.Counter))  # comuna -> clave -> nombres
    rutas = collections.defaultdict(lambda: collections.defaultdict(list))    # cut -> ruta -> [coords]
    nodo_nombres = collections.defaultdict(set)
    nodo_xy = {}
    for iv, (nm, ref, pts) in enumerate(lec.vias):
        coords = [(lon, lat) for _, lon, lat in pts]
        cset = comunas_via.get(iv, set())
        # Claves de la via: su nombre y, si tiene codigo de ruta, "ruta X" (asi "Ruta A-16 & Circunvalacion"
        # encuentra el cruce aunque el tramo no tenga nombre en OSM)
        claves_via = {}
        if nm:
            claves_via[clave_osm(nm)] = nm
        if ref:
            for parte in str(ref).split(";"):
                rc = limpiar_ruta(parte.strip())[0]
                if rc:
                    claves_via.setdefault(clave_osm(f"Ruta {rc}"), f"Ruta {rc}")
        for k, original in claves_via.items():
            nombre_osm[k][original] += 1
            for c in cset:
                calles[c][k].append(coords)
                nombre_osm_c[c][k][original] += 1
            for nid, lon, lat in pts:
                nodo_nombres[nid].add(k)
                nodo_xy[nid] = (lon, lat)
        if ref:
            for parte in str(ref).split(";"):
                rc = limpiar_ruta(parte.strip())[0]
                if rc:
                    for c in cset:
                        rutas[c][rc].append(coords)

    # Cruces: nodos compartidos por calles de nombre distinto
    cruces_n = [(nid, ns) for nid, ns in nodo_nombres.items() if len(ns) >= 2]
    com_cruce = comuna_de([nodo_xy[n][0] for n, _ in cruces_n], [nodo_xy[n][1] for n, _ in cruces_n])
    cruces = collections.defaultdict(dict)                                    # cut -> (a, b) -> (lon, lat)
    for (nid, ns), c in zip(cruces_n, com_cruce):
        if not c:
            continue
        ns = sorted(ns)
        for i in range(len(ns)):
            for j in range(i + 1, len(ns)):
                cruces[c].setdefault((ns[i], ns[j]), nodo_xy[nid])
    del nodo_nombres, nodo_xy

    # Numeros de casa por comuna y calle
    com_dir = comuna_de([d[2] for d in lec.direcciones], [d[3] for d in lec.direcciones])
    numeros = collections.defaultdict(lambda: collections.defaultdict(list))
    for (calle, num, lon, lat), c in zip(lec.direcciones, com_dir):
        if c:
            numeros[c][clave_osm(calle)].append((num, lon, lat))
    for c in numeros:
        for k in numeros[c]:
            numeros[c][k].sort()

    # Hitos kilometricos por ruta y localidades por comuna
    hitos = collections.defaultdict(list)
    _h = [h for h in lec.hitos.values() if h[3]]
    for (d, lon, lat, ref), c in zip(_h, comuna_de([h[1] for h in _h], [h[2] for h in _h])):
        rc = limpiar_ruta(str(ref).split(";")[0])[0]
        if rc and c:
            hitos[rc].append((d, lon, lat, c))
    for rc in hitos:
        hitos[rc].sort()
    com_lug = comuna_de([x[1] for x in lec.lugares], [x[2] for x in lec.lugares])
    lugares = collections.defaultdict(dict)
    for (nm, lon, lat), c in zip(lec.lugares, com_lug):
        if c:
            lugares[c].setdefault(clave_osm(nm), (lon, lat, nm))

    indice = {
        "calles": {c: dict(v) for c, v in calles.items()},
        "nombre_osm": {k: v.most_common(1)[0][0] for k, v in nombre_osm.items()},
        "nombre_osm_c": {c: {k: v.most_common(1)[0][0] for k, v in d.items()} for c, d in nombre_osm_c.items()},
        "rutas": {c: dict(v) for c, v in rutas.items()},
        "cruces": dict(cruces), "numeros": {c: dict(v) for c, v in numeros.items()},
        "hitos": dict(hitos), "lugares": dict(lugares), "vecinas": vecinas,
        "poligonos": [(c, shapely.to_wkb(g)) for c, g in zip(cuts, poligonos)],
        "fuente": {"archivo": OSM_PBF.name, "bytes": OSM_PBF.stat().st_size, "mtime": OSM_PBF.stat().st_mtime},
        "version": OSM_INDICE_VERSION,
    }
    glog.info("Indice OSM: %s calles por comuna, %s cruces, %s calles con numeros, %s rutas con hitos (%s)",
              miles(sum(len(v) for v in indice["calles"].values())), miles(sum(len(v) for v in indice["cruces"].values())),
              miles(sum(len(v) for v in indice["numeros"].values())), miles(len(indice["hitos"])),
              duracion(time.time() - t0))
    return indice


def descargar_mop() -> list:
    """Descarga la red vial del MOP por paginas de 1000 tramos. Devuelve [(rol, nombre, [[(lon, lat, m)]])]."""
    MOP_DIR.mkdir(parents=True, exist_ok=True)
    if MOP_DATOS.exists() and (time.time() - MOP_DATOS.stat().st_mtime) / 86400 <= MOP_MAX_DIAS and not OSM_ACTUALIZAR:
        import pickle
        with MOP_DATOS.open("rb") as f:
            return pickle.load(f)
    glog.info("Descargando red vial MOP: %s", MOP_URL)
    tramos, offset, t0 = [], 0, time.time()
    while True:
        params = {"where": "1=1", "outFields": "ROL,NOMBRE", "returnM": "true", "returnGeometry": "true",
                  "outSR": "4326", "geometryPrecision": "6", "resultOffset": offset, "resultRecordCount": 1000,
                  "orderByFields": "OBJECTID", "f": "json"}
        datos = None
        for intento in range(1, REINTENTOS + 2):
            try:
                rq = requests.get(MOP_URL, params=params, timeout=TIMEOUT_SEC)
                rq.raise_for_status()
                datos = rq.json()
                if "error" in datos:
                    raise RuntimeError(datos["error"])
                break
            except Exception as exc:
                glog.warning("  pagina %d con error (intento %d): %s", offset // 1000 + 1, intento, exc)
                time.sleep(5 * intento)
        if datos is None:
            raise RuntimeError("No se pudo descargar la red vial del MOP.")
        feats = datos.get("features", [])
        for ft in feats:
            a, g = ft.get("attributes", {}), ft.get("geometry") or {}
            caminos = [[(pt[0], pt[1], pt[2]) for pt in camino if len(pt) >= 3 and pt[2] is not None]
                       for camino in g.get("paths", [])]
            tramos.append(((a.get("ROL") or "").strip(), a.get("NOMBRE"), [c for c in caminos if len(c) >= 2]))
        glog.info("  %s tramos descargados (%s)", miles(len(tramos)), duracion(time.time() - t0))
        if len(feats) < 1000:
            break
        offset += 1000
    import pickle
    with MOP_DATOS.open("wb") as f:
        pickle.dump(tramos, f, protocol=pickle.HIGHEST_PROTOCOL)
    return tramos


def indice_mop(idx_osm: dict) -> dict:
    """Tramos del MOP por (comuna, ruta), listos para interpolar la medida M. Se guarda en disco."""
    import pickle
    import shapely
    from shapely.strtree import STRtree
    tramos = descargar_mop()
    clave = (MOP_DATOS.stat().st_mtime, idx_osm.get("fuente", {}).get("mtime"), OSM_INDICE_VERSION)
    if MOP_INDICE.exists() and not OSM_ACTUALIZAR:
        with MOP_INDICE.open("rb") as f:
            im = pickle.load(f)
        if im.get("clave") == clave:
            glog.info("Indice MOP leido desde %s", MOP_INDICE)
            return im
    t0 = time.time()
    cuts = [c for c, _ in idx_osm["poligonos"]]
    arbol = STRtree([shapely.from_wkb(w) for _, w in idx_osm["poligonos"]])
    por_comuna = collections.defaultdict(lambda: collections.defaultdict(list))
    sin_rol = 0
    for rol, nombre, caminos in tramos:
        rc = limpiar_ruta(rol)[0] if rol else None
        if not rc:
            sin_rol += 1
            continue
        for cam in caminos:
            ms = [p[2] for p in cam]
            creciente = all(b >= a for a, b in zip(ms, ms[1:]))
            paso = max(1, len(cam) // 40)
            muestra = cam[::paso] + [cam[-1]]
            pts = shapely.points([p[0] for p in muestra], [p[1] for p in muestra])
            ip, ig = arbol.query(pts, predicate="within")
            for c in {cuts[g] for g in ig}:
                por_comuna[c][rc].append((ms, [(p[0], p[1]) for p in cam], creciente))
    im = {"rutas": {c: dict(v) for c, v in por_comuna.items()}, "clave": clave}
    with MOP_INDICE.open("wb") as f:
        pickle.dump(im, f, protocol=pickle.HIGHEST_PROTOCOL)
    glog.info("Indice MOP: %s tramos, %s sin rol utilizable, %s rutas por comuna (%s)", miles(len(tramos)), miles(sin_rol),
              miles(sum(len(v) for v in im["rutas"].values())), duracion(time.time() - t0))
    return im


def leer_csv_usuario(path: Path) -> pd.DataFrame:
    """CSV editado a mano (Excel puede guardarlo en ANSI o con coma): se prueban codificacion y separador."""
    for enc in ("utf-8-sig", "cp1252"):
        for sep in (";", ","):
            try:
                df = pd.read_csv(path, sep=sep, dtype=str, encoding=enc, keep_default_na=False)
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
            if "nombre_dato" in df.columns:
                return df
    raise ValueError(f"{path.name}: no se reconoce el formato (falta la columna nombre_dato)")


def cargar_alias_manual() -> dict:
    """
    Lee alias_calles.csv y absorbe lo completado en alias_sugeridos.csv. Devuelve
    {"alias": {(cut, clave): clave_osm | BLOQUEO}, "puntos": {(cut, clave): (lon, lat, precision)}}.
    cut vacio ("") significa que el alias vale para todas las comunas.
    """
    sug = GEO_DIR / "alias_sugeridos.csv"
    base = leer_csv_usuario(ALIAS_MANUAL) if ALIAS_MANUAL.exists() else pd.DataFrame(columns=COLS_ALIAS)
    if sug.exists():
        try:
            s_df = leer_csv_usuario(sug)
            for c in COLS_ALIAS:
                s_df[c] = s_df.get(c, "")
            hechos = s_df[(s_df["nombre_osm"].str.strip() != "") | (s_df["lat"].str.strip() != "")][COLS_ALIAS]
            if len(hechos):
                base = pd.concat([base, hechos], ignore_index=True)
                base = base.drop_duplicates(["cod_comuna", "nombre_dato"], keep="last")
                glog.info("Alias completados en alias_sugeridos.csv y copiados a %s: %d", ALIAS_MANUAL.name, len(hechos))
        except Exception as exc:
            glog.warning("No se pudo leer %s: %s", sug.name, exc)
    for c in COLS_ALIAS:
        if c not in base.columns:
            base[c] = ""
    # solo se reescribe si es nuevo o si absorbio filas (asi no choca con el archivo abierto en Excel)
    if not ALIAS_MANUAL.exists() or (sug.exists() and "hechos" in locals() and len(hechos)):
        escribir_csv_seguro(base[COLS_ALIAS + [c for c in base.columns if c not in COLS_ALIAS]].fillna(""), ALIAS_MANUAL)

    alias, puntos = {}, {}
    for f in base.fillna("").itertuples(index=False):
        k = clave_osm(f.nombre_dato)
        if not k:
            continue
        cod = re.sub(r"\D", "", str(f.cod_comuna))
        cut = cod.zfill(5) if cod else ""
        destino = str(f.nombre_osm).strip()
        if destino.upper() in ("-", "NINGUNO", "NINGUNA", "BLOQUEAR"):
            alias[(cut, k)] = BLOQUEO
        elif destino:
            alias[(cut, k)] = clave_osm(destino)
        try:
            lat = float(str(f.lat).replace(",", "."))
            lon = float(str(f.lon).replace(",", "."))
            prec = float(str(f.precision_m).replace(",", ".")) if str(f.precision_m).strip() else PRECISION_M["punto manual"]
            if -56 < lat < -17 and -110 < lon < -66:
                puntos[(cut, k)] = (lon, lat, prec)
        except ValueError:
            pass
    glog.info("Alias manuales: %d nombres (%d bloqueos) y %d puntos manuales desde %s", len(alias),
              sum(v == BLOQUEO for v in alias.values()), len(puntos), ALIAS_MANUAL)
    return {"alias": alias, "puntos": puntos}


class Geocodificador:
    """Resuelve direcciones de la etapa 7 contra el indice OSM, siempre dentro de la comuna del accidente."""

    def __init__(self, idx: dict, mop: dict | None = None, manual: dict | None = None):
        from rapidfuzz import process, fuzz
        import shapely
        from shapely.strtree import STRtree
        self.idx, self._process, self._fuzz = idx, process, fuzz
        self.mop = mop or {"rutas": {}}
        self.manual = manual or {"alias": {}, "puntos": {}}
        self.ejemplo: dict = {}                      # (cut, clave) -> como venia escrito el nombre
        self.alias_invalidos = collections.Counter()  # alias manuales cuyo nombre OSM no existe en la comuna
        self._cuts_pol = [c for c, _ in idx.get("poligonos", [])]
        self._pol = [shapely.from_wkb(w) for _, w in idx.get("poligonos", [])]
        self._arbol_pol = STRtree(self._pol) if self._pol else None
        self._nombres = {c: list(v) for c, v in idx["calles"].items()}
        self._cache_nombre: dict = {}
        self._mapas_c: dict = {}
        self._cache_cont: dict = {}
        self.vias_tipo = collections.Counter()      # forma en que se encontro cada nombre (primera vez por comuna)
        self._cache_var: dict = {}
        self._cache_geom: dict = {}
        self._tokens: dict = {}
        self.aproximados = collections.Counter()   # (comuna, clave dato, clave osm) -> filas
        self.sin_pareja = collections.Counter()    # (comuna, clave dato) -> filas

    def _comunas(self, cut):
        return [cut] + sorted(self.idx["vecinas"].get(cut, ()))

    def _mapas(self, c):
        """Mapas auxiliares por comuna (se arman la primera vez que se consulta la comuna)."""
        if c not in self._mapas_c:
            compacto, nucleo, inv = collections.defaultdict(set), collections.defaultdict(set), collections.defaultdict(set)
            for x in self._nombres.get(c, []):
                compacto[x.replace(" ", "")].add(x)
                ncl = clave_nucleo(x)
                nucleo[ncl].add(x)
                for t in ncl.split():
                    if len(t) >= 4:
                        inv[t].add(ncl)
            self._mapas_c[c] = (compacto, nucleo, inv)
        return self._mapas_c[c]

    def _contenido(self, k, c, todos=False):
        """
        Calle cuyo nucleo contiene al del dato, o al reves ('prat' y 'arturo prat'; 'manuel balmaceda' y
        'balmaceda'). Solo se acepta si hay un candidato unico con menos palabras de diferencia y comparten
        al menos una palabra de 4 letras o mas; numeros y direccionales deben coincidir.
        """
        llave = (k, c, todos)
        if llave in self._cache_cont:
            return self._cache_cont[llave]
        nk = set(clave_nucleo(k).split())
        if not nk or not any(len(t) >= 4 for t in nk):
            return [] if todos else None
        _, nucleo, inv = self._mapas(c)
        # solo se revisan los nucleos que comparten alguna palabra de 4 letras o mas
        posibles = set().union(*(inv.get(t, set()) for t in nk if len(t) >= 4))
        todos_c = []
        for ncl in posibles:
            claves = nucleo[ncl]
            st = set(ncl.split())
            if not st or not (nk <= st or st <= nk):
                continue
            if not any(len(t) >= 4 for t in nk & st):
                continue
            if {t for t in nk if t.isdigit()} != {t for t in st if t.isdigit()} or (nk & DIRECCIONALES) != (st & DIRECCIONALES):
                continue
            # lo compartido debe incluir una palabra distintiva ("chile" o "central" no bastan)
            if not any(len(t) >= 4 and t not in GENERICAS for t in nk & st):
                continue
            if st < nk:
                # OSM mas corto que el dato: debe conservar la ultima palabra o, en nombres de 3 o mas
                # palabras, el apellido paterno ('jorge alessandri rodriguez' -> 'jorge alessandri'), pero no
                # quedarse solo con el nombre de pila ('jose asencio' no es 'san jose')
                L = clave_nucleo(k).split()
                if not (L[-1] in st or (len(L) >= 3 and L[1] in st and len(st) >= 2)):
                    continue
            tit_dato = set(k.split()) & TITULOS
            for x in claves:
                tit_osm = set(x.split()) & TITULOS
                if st < nk and tit_osm - tit_dato:      # 'vicuna mackenna' no es 'general mackenna'
                    continue
                if nk < st and len(nk) == 1 and tit_dato - tit_osm:   # 'general velasquez' no es 'maria rozas velasquez'
                    continue
                todos_c.append((len(nk ^ st), x))
        todos_c.sort()
        if todos:  # para cruces: todos los candidatos con hasta 2 palabras de diferencia
            res = [x for d, x in todos_c if d <= 2][:6]
        elif todos_c:
            mejores = [x for d, x in todos_c if d == todos_c[0][0]]
            res = self._unico(mejores, c)
        else:
            res = None
        self._cache_cont[llave] = res
        return res

    def _unico(self, claves, c):
        """Una sola clave, o la de mas tramos si todas comparten el mismo nucleo (son la misma calle)."""
        claves = sorted(set(claves))
        if len(claves) == 1:
            return claves[0]
        if claves and len({clave_nucleo(x) for x in claves}) == 1:
            cc = self.idx["calles"].get(c, {})
            return max(claves, key=lambda x: len(cc.get(x, ())))
        return None

    def _iniciales(self, k, c):
        """
        Nombres con iniciales: 'v mackenna' -> 'vicuna mackenna', 'p a cerda' -> 'pedro aguirre cerda',
        'j j perez' -> 'jose joaquin perez'. Las palabras largas deben estar todas y cada inicial debe
        corresponder a otra palabra del nombre OSM; se exige un candidato unico.
        """
        toks = k.split()
        ini = [t for t in toks if len(t) == 1 and t.isalpha()]
        largas = [t for t in toks if len(t) >= 4]
        if not ini or not largas:
            return None
        cifras = sorted(t for t in toks if t.isdigit())   # "5 1 2 poniente f" no es "5 poniente f"
        _, nucleo, inv = self._mapas(c)
        posibles = set.intersection(*(inv.get(t, set()) for t in largas))
        hallados = []
        for ncl in posibles:
            for x in nucleo[ncl]:
                if sorted(t for t in x.split() if t.isdigit()) != cifras:
                    continue
                resto = [t for t in x.split() if t not in largas and t not in PALABRAS_VACIAS]
                if len(resto) < len(ini):
                    continue
                pendientes = list(resto)
                ok = True
                for i0 in ini:
                    m = next((t for t in pendientes if t.startswith(i0)), None)
                    if m is None:
                        ok = False
                        break
                    pendientes.remove(m)
                if ok and len(pendientes) <= 1:
                    hallados.append(x)
        return self._unico(hallados, c)

    def candidatos(self, via, c):
        """Todas las claves OSM plausibles para la calle en una comuna (para probar cruces alternativos)."""
        k = clave_osm(via)
        cc = self.idx["calles"].get(c, {})
        out = [x for x in (k, alias_osm(k, c)) if x and x in cc]
        compacto, nucleo, _ = self._mapas(c)
        out += sorted(compacto.get(k.replace(" ", ""), ())) + sorted(nucleo.get(clave_nucleo(k), ()))
        out += self._contenido(k, c, todos=True) or []
        return list(dict.fromkeys(out))[:8]

    def _aproximado(self, k, c):
        """Coincidencia aproximada con resguardos: mismos numeros, mismas direccionales, umbral mayor si es corto."""
        if not self._nombres.get(c):
            return None
        umbral = 95 if len(k) <= 7 else UMBRAL_NOMBRE
        for cand, puntaje, _ in self._process.extract(k, self._nombres[c], scorer=self._fuzz.token_sort_ratio,
                                                      score_cutoff=umbral, limit=3):
            a, b = set(k.split()), set(cand.split())
            if {t for t in a if t.isdigit()} != {t for t in b if t.isdigit()}:
                continue
            if (a & DIRECCIONALES) != (b & DIRECCIONALES):
                continue
            if {t for t in a if len(t) == 1} != {t for t in b if len(t) == 1}:   # "5 1/2 poniente f" no es "... b"
                continue
            return cand
        return None

    def nombre(self, via, cut, n=1):
        """
        Clave OSM de la calle en la comuna (o vecinas). Devuelve (clave, comuna, tipo). Orden de busqueda:
        exacto, alias, sin espacios (Mac Iver = MacIver), nucleo sin titulos, nombre contenido y, solo en la
        comuna del accidente, coincidencia aproximada.
        """
        k = clave_osm(via)
        llave = (k, cut)
        if llave in self._cache_nombre:
            return self._cache_nombre[llave]
        res = (None, None, None)
        comunas = self._comunas(cut)
        # 1) alias manual (alias_calles.csv): manda sobre todo lo demas, incluso para bloquear
        man = self.manual["alias"].get((cut, k), self.manual["alias"].get(("", k)))
        if man == BLOQUEO:
            res = (None, None, "bloqueado")
        elif man:
            for i, c in enumerate(comunas):
                if man in self.idx["calles"].get(c, {}):
                    res = (man, c, "alias manual" + ("" if i == 0 else " (comuna vecina)"))
                    break
            else:
                self.alias_invalidos[(cut, k, man)] += n
        if res[2]:
            self._cache_nombre[llave] = res
            self.vias_tipo[res[2].split(" (")[0]] += n
            return res
        alias = alias_osm(k, cut)
        for paso in ("exacto", "alias", "sin espacios", "nucleo", "contenido", "iniciales", "sin direccional"):
            for i, c in enumerate(comunas):
                cc = self.idx["calles"].get(c, {})
                hallado = None
                if paso == "exacto" and k in cc:
                    hallado = k
                elif paso == "alias" and alias and alias in cc:
                    hallado = alias
                elif paso == "sin espacios":
                    cand = self._mapas(c)[0].get(k.replace(" ", ""), set())
                    # "16 1 2 norte" no es "161 2 norte": los numeros deben ser los mismos
                    cand = {x for x in cand if re.findall(r"\d+", x) == re.findall(r"\d+", k)}
                    hallado = next(iter(cand)) if len(cand) == 1 else None
                elif paso == "nucleo":
                    ncl = clave_nucleo(k)
                    # un nucleo hecho solo de numeros ("caletera a 16" -> "16") no identifica una calle
                    if ncl != k and all(t.isdigit() or t in DIRECCIONALES for t in ncl.split()):
                        hallado = None
                    else:
                        hallado = self._unico(self._mapas(c)[1].get(ncl, set()), c)
                elif paso == "iniciales":
                    hallado = self._iniciales(k, c)
                elif paso == "sin direccional":
                    # "las rejas norte" -> "las rejas" cuando OSM no distingue el tramo
                    toks = k.split()
                    if len(toks) >= 2 and toks[-1] in DIRECCIONALES:
                        base = " ".join(toks[:-1])
                        if any(len(t) >= 4 and t not in GENERICAS and not t.isdigit() for t in toks[:-1]) and base in cc:
                            hallado = base
                elif paso == "contenido":
                    hallado = self._contenido(k, c)
                if hallado:
                    res = (hallado, c, paso + ("" if i == 0 else " (comuna vecina)"))
                    break
            if res[0]:
                break
        if res[0] is None:
            cand = self._aproximado(k, cut)
            if cand:
                res = (cand, cut, "aproximado")
        self._cache_nombre[llave] = res
        self.vias_tipo[res[2].split(" (")[0] if res[2] else "sin pareja"] += n
        return res

    def punto_manual(self, via, cut):
        k = clave_osm(via)
        return self.manual["puntos"].get((cut, k), self.manual["puntos"].get(("", k)))

    def _registrar(self, via, cut, res, n):
        k = clave_osm(via)
        self.ejemplo.setdefault((cut, k), via)
        if res[0] is None and res[2] != "bloqueado" and not self.punto_manual(via, cut):
            self.sin_pareja[(cut, k)] += n
        elif res[2] and res[2].split(" (")[0] in ("aproximado", "contenido", "iniciales", "sin direccional", "nucleo",
                                                   "sin espacios", "alias", "alias manual"):
            self.aproximados[(cut, k, res[0], res[2])] += n

    def _punto_medio_calle(self, k, c):
        from shapely.geometry import MultiLineString
        g = MultiLineString([l for l in self.idx["calles"][c][k] if len(l) >= 2])
        if g.is_empty:
            return None
        p = g.interpolate(0.5, normalized=True)
        return p.x, p.y

    def variantes(self, k, c):
        """Nombres OSM de la comuna que contienen la calle como palabras completas (Quilin Sur, Rotonda Quilin)."""
        llave = (k, c)
        if llave not in self._cache_var:
            if c not in self._tokens:
                inv = collections.defaultdict(set)
                for x in self._nombres.get(c, []):
                    for tok in x.split():
                        inv[tok].add(x)
                self._tokens[c] = inv
            inv = self._tokens[c]
            toks = k.split()
            cand = set.intersection(*(inv.get(t, set()) for t in toks)) if toks else set()
            patron = re.compile(rf"(?:^|\s){re.escape(k)}(?:\s|$)")
            self._cache_var[llave] = sorted(x for x in cand if x != k and len(x.split()) - len(toks) <= 2
                                            and patron.search(x))[:8]
        return [k] + self._cache_var[llave]

    def _geom(self, c, claves):
        from shapely.geometry import MultiLineString
        llave = (c, tuple(claves))
        if llave not in self._cache_geom:
            cc = self.idx["calles"].get(c, {})
            lineas = [l for k in claves for l in cc.get(k, []) if len(l) >= 2]
            self._cache_geom[llave] = MultiLineString(lineas) if lineas else None
            if len(self._cache_geom) > 200_000:
                self._cache_geom.clear()
        return self._cache_geom[llave]

    def cruce(self, v1, v2, cut, n):
        r1, r2 = self.nombre(v1, cut), self.nombre(v2, cut)
        self._registrar(v1, cut, r1, n)
        self._registrar(v2, cut, r2, n)
        if not r1[0] or not r2[0]:
            return None
        for c in self._comunas(cut):
            cr = self.idx["cruces"].get(c, {})
            par = tuple(sorted((r1[0], r2[0])))
            if par in cr:
                lon, lat = cr[par]
                return lon, lat, "cruce exacto", r1, r2
            for a in self.variantes(r1[0], c):
                for b in self.variantes(r2[0], c):
                    par = tuple(sorted((a, b)))
                    if par in cr:
                        lon, lat = cr[par]
                        return lon, lat, "cruce exacto (variante de nombre)", r1, r2
        # Sin nodo comun (pasos a desnivel, autopistas con caleteras): distancia minima entre calles
        from shapely.ops import nearest_points
        for c in self._comunas(cut):
            g1 = self._geom(c, self.variantes(r1[0], c))
            g2 = self._geom(c, self.variantes(r2[0], c))
            if g1 is None or g2 is None:
                continue
            a, b = nearest_points(g1, g2)
            if metros(a.x, a.y, b.x, b.y) <= CRUCE_MAX_M:
                return (a.x + b.x) / 2, (a.y + b.y) / 2, "cruce por cercania", r1, r2
        # Nombres alternativos plausibles (por ejemplo "Prat" puede ser "Arturo Prat" o "Capitan Prat")
        for c in self._comunas(cut):
            cr = self.idx["cruces"].get(c, {})
            for a in self.candidatos(v1, c):
                for b in self.candidatos(v2, c):
                    par = tuple(sorted((a, b)))
                    if a != b and par in cr:
                        lon, lat = cr[par]
                        return lon, lat, "cruce exacto (nombre alternativo)", (a, c, "alternativo"), (b, c, "alternativo")
        return None

    def numero(self, via, num, cut, n):
        r = self.nombre(via, cut)
        self._registrar(via, cut, r, n)
        if not r[0]:
            pm = self.punto_manual(via, cut)
            return (pm[0], pm[1], "punto manual", ("__manual__", cut, "manual")) if pm else None
        try:
            objetivo = int(re.sub(r"\D", "", str(num)))
        except ValueError:
            objetivo = None
        lista = self.idx["numeros"].get(r[1], {}).get(r[0], [])
        if objetivo and lista:
            exactos = [x for x in lista if x[0] == objetivo]
            if exactos:
                return exactos[0][1], exactos[0][2], "numero exacto", r
            # 1) misma vereda (misma paridad) y tramo corto; 2) cualquier vereda y tramo de hasta 1000 numeros
            for max_dif, paridad, metodo, max_m in ((NUM_MAX_DIF, True, "numero interpolado", 1000),
                                                    (1000, False, "numero interpolado (tramo largo)", 2500)):
                menores = [x for x in lista if x[0] < objetivo and objetivo - x[0] <= max_dif
                           and (not paridad or x[0] % 2 == objetivo % 2)]
                mayores = [x for x in lista if x[0] > objetivo and x[0] - objetivo <= max_dif
                           and (not paridad or x[0] % 2 == objetivo % 2)]
                if menores and mayores:
                    a, b = menores[-1], mayores[0]
                    if metros(a[1], a[2], b[1], b[2]) < max_m:
                        f = (objetivo - a[0]) / (b[0] - a[0])
                        return a[1] + f * (b[1] - a[1]), a[2] + f * (b[2] - a[2]), metodo, r
            cerca = min(lista, key=lambda x: abs(x[0] - objetivo))
            if abs(cerca[0] - objetivo) <= 50:
                return cerca[1], cerca[2], "numero cercano", r
        p = self._punto_medio_calle(r[0], r[1])
        return (p[0], p[1], "calle en la comuna", r) if p else None

    def calle(self, via, cut, n):
        if str(via).lower().startswith("sector "):
            k = clave_osm(via)
            for c in self._comunas(cut):
                if k in self.idx["lugares"].get(c, {}):
                    lon, lat, nm = self.idx["lugares"][c][k]
                    return lon, lat, "localidad", ("__lugar__" + nm, c, "exacto")
        r = self.nombre(via, cut)
        self._registrar(via, cut, r, n)
        if not r[0]:
            pm = self.punto_manual(via, cut)
            return (pm[0], pm[1], "punto manual", ("__manual__", cut, "manual")) if pm else None
        p = self._punto_medio_calle(r[0], r[1])
        return (p[0], p[1], "calle en la comuna", r) if p else None

    def _comuna_punto(self, lon, lat):
        import shapely
        if self._arbol_pol is None:
            return None
        p = shapely.Point(lon, lat)
        for i in self._arbol_pol.query(p, predicate="within"):
            return self._cuts_pol[i]
        i = self._arbol_pol.query_nearest(p, max_distance=0.01)
        return self._cuts_pol[i[0]] if len(i) else None

    def km_mop(self, ruta, kmf, cut):
        """Punto de la ruta donde la medida M del MOP vale km * 1000, dentro de la comuna o sus vecinas."""
        import bisect
        objetivo = kmf * 1000
        candidatos = []
        for orden, c in enumerate(self._comunas(cut)):
            for ms, xy, creciente in self.mop["rutas"].get(c, {}).get(ruta, []):
                if not (min(ms[0], ms[-1]) - 50 <= objetivo <= max(ms[0], ms[-1]) + 50):
                    continue
                if creciente:
                    j = min(max(bisect.bisect_left(ms, objetivo), 1), len(ms) - 1)
                    tramos = [j]
                else:
                    tramos = [j for j in range(1, len(ms)) if min(ms[j - 1], ms[j]) <= objetivo <= max(ms[j - 1], ms[j])]
                for j in tramos:
                    m0, m1 = ms[j - 1], ms[j]
                    f = 0.0 if m1 == m0 else min(max((objetivo - m0) / (m1 - m0), 0.0), 1.0)
                    lon = xy[j - 1][0] + f * (xy[j][0] - xy[j - 1][0])
                    lat = xy[j - 1][1] + f * (xy[j][1] - xy[j - 1][1])
                    candidatos.append((orden, lon, lat))
            if candidatos and orden == 0:
                break
        permitidas = set(self._comunas(cut))
        for orden, lon, lat in sorted(candidatos):
            com = self._comuna_punto(lon, lat)
            if com in permitidas:
                return lon, lat
        return None

    def ruta_km(self, ruta, km, cut):
        from shapely.geometry import MultiLineString, Point
        lineas = []
        for c in self._comunas(cut):
            lineas += self.idx["rutas"].get(c, {}).get(ruta, [])
            if lineas and c == cut:
                break
        lineas = [l for l in lineas if len(l) >= 2]
        if km is not None:
            try:
                kmf = float(km)
            except ValueError:
                kmf = None
            # solo hitos de la comuna o sus vecinas: el mismo km existe al norte y al sur de Santiago
            permitidas = set(self._comunas(cut))
            hitos = [h for h in self.idx["hitos"].get(ruta, []) if h[3] in permitidas]
            if kmf is not None and hitos:
                bajo = [h for h in hitos if h[0] <= kmf]
                alto = [h for h in hitos if h[0] >= kmf]
                if bajo and abs(bajo[-1][0] - kmf) < 0.05:
                    return bajo[-1][1], bajo[-1][2], "km en hito"
            if kmf is not None:
                pm = self.km_mop(ruta, kmf, cut)
                if pm:
                    lon, lat = pm
                    if lineas:  # la red MOP es escala 1:350.000: se ajusta a la geometria OSM si esta cerca
                        g = MultiLineString(lineas)
                        q = g.interpolate(g.project(Point(lon, lat)))
                        if metros(lon, lat, q.x, q.y) < 1500:
                            lon, lat = q.x, q.y
                    return lon, lat, "km sobre red vial MOP"
            if kmf is not None and hitos:
                if bajo and alto and alto[0][0] - bajo[-1][0] <= 20:
                    a, b = bajo[-1], alto[0]
                    f = (kmf - a[0]) / (b[0] - a[0]) if b[0] != a[0] else 0
                    lon, lat = a[1] + f * (b[1] - a[1]), a[2] + f * (b[2] - a[2])
                    if lineas:  # se ajusta el punto a la geometria de la ruta
                        g = MultiLineString(lineas)
                        p = g.interpolate(g.project(Point(lon, lat)))
                        lon, lat = p.x, p.y
                    return lon, lat, "km entre hitos"
        if lineas:
            g = MultiLineString(lineas)
            p = g.interpolate(0.5, normalized=True)
            return p.x, p.y, "ruta en la comuna"
        return None


if DESDE <= 8 <= HASTA:
    _t0 = time.time()
    GEO_DIR.mkdir(parents=True, exist_ok=True)
    if not glog.handlers:
        _fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        for _h in (logging.FileHandler(GEO_LOG_PATH, encoding="utf-8"), logging.StreamHandler()):
            _h.setFormatter(_fmt)
            glog.addHandler(_h)
    log.info("=== ETAPA 8: coordenadas con OpenStreetMap (log de depuracion en %s) ===", GEO_LOG_PATH)
    glog.info("=== ETAPA 8: coordenadas con OpenStreetMap ===")
    _ok = _fa = 0
    try:
        import pickle
        _faltan = [m for m in ("osmium", "shapely", "rapidfuzz") if importlib.util.find_spec(m) is None]
        if _faltan:
            raise RuntimeError(f"Faltan librerias para la etapa 8 en este Python ({sys.executable}). "
                               f"Instalalas con: \"{sys.executable}\" -m pip install {' '.join(_faltan)}")
        descargar_osm()
        _idx = None
        if OSM_INDICE.exists() and not OSM_ACTUALIZAR:
            with OSM_INDICE.open("rb") as _f:
                _idx = pickle.load(_f)
            if (_idx.get("fuente", {}).get("mtime") != OSM_PBF.stat().st_mtime
                    or _idx.get("version") != OSM_INDICE_VERSION):
                glog.info("El extracto OSM o la version del indice cambio: se reconstruye el indice")
                _idx = None
            else:
                glog.info("Indice OSM leido desde %s", OSM_INDICE)
        if _idx is None:
            _idx = construir_indice()
            with OSM_INDICE.open("wb") as _f:
                pickle.dump(_idx, _f, protocol=pickle.HIGHEST_PROTOCOL)
            glog.info("Indice OSM guardado en %s (%.0f MB)", OSM_INDICE, OSM_INDICE.stat().st_size / 1024 / 1024)

        _ruta_dir = DIR_DIR / "direcciones.csv"
        if not _ruta_dir.exists():
            raise RuntimeError(f"No existe {_ruta_dir}: corre antes la etapa 7.")
        _dir = leer_csv(_ruta_dir)
        if "via_1" not in _dir.columns:
            raise RuntimeError("direcciones.csv no trae los componentes: vuelve a correr la etapa 7.")
        glog.info("Direcciones leidas: %s filas", miles(len(_dir)))

        _claves = ["cod_comuna", "tipo_direccion"] + COMPONENTES_DIR
        _q = _dir.dropna(subset=["direccion", "cod_comuna"])[_claves].fillna(SENT).value_counts().reset_index(name="n")
        glog.info("Consultas distintas (componentes + comuna): %s", miles(len(_q)))
        try:
            _mop = indice_mop(_idx)
        except Exception as exc:  # sin red MOP la etapa sigue, con hitos OSM para los km
            glog.warning("Red vial MOP no disponible (%s): los km se ubicaran solo con hitos de OSM", exc)
            _mop = None
        _manual = cargar_alias_manual()
        _geo = Geocodificador(_idx, _mop, _manual)
        _sal = []
        _tq = time.time()
        for _i, _f in enumerate(_q.itertuples(index=False)):
            v1, v2, num, ruta, km = [None if x == SENT else x for x in (_f.via_1, _f.via_2, _f.numero, _f.ruta_cod, _f.km_val)]
            cut, tipo, n = str(_f.cod_comuna).zfill(5), _f.tipo_direccion, _f.n
            r = None
            via_osm = (None, None)
            try:
                if tipo == "interseccion":
                    r = _geo.cruce(v1, v2, cut, n)
                    if r:
                        via_osm = (r[3][0], r[4][0])
                        r = r[:3]
                    else:  # sin cruce: al menos la primera calle que exista en la comuna
                        r2 = _geo.calle(v1, cut, n) or _geo.calle(v2, cut, n)
                        r = r2[:3] if r2 else None
                elif tipo == "numero":
                    r = _geo.numero(v1, num, cut, n)
                    if r:
                        via_osm, r = (r[3][0], None), r[:3]
                elif tipo in ("solo_calle", "via_km"):
                    r = _geo.calle(v1, cut, n)
                    if r:
                        via_osm, r = (r[3][0], None), r[:3]
                elif tipo in ("ruta_km", "ruta"):
                    r = _geo.ruta_km(ruta, km, cut)
            except Exception as exc:  # una consulta mala no debe detener la etapa
                glog.warning("Consulta con error (%s | %s): %s", cut, (v1, v2, num, ruta, km), exc)
                r = None
            if r:
                lon, lat, metodo = r
                d_osm = None
                _nc = _idx.get("nombre_osm_c", {}).get(cut, {})
                _nom = lambda k_, defecto: _nc.get(k_) or _idx["nombre_osm"].get(k_, defecto)  # noqa: E731
                if via_osm[0] and tipo == "interseccion" and via_osm[1]:
                    d_osm = FMT_INTERSECCION.format(c1=_nom(via_osm[0], v1), c2=_nom(via_osm[1], v2))
                elif via_osm[0] and tipo == "numero":
                    d_osm = FMT_NUMERO.format(c1=_nom(via_osm[0], v1), n=num)
                elif via_osm[0] and via_osm[0].startswith("__lugar__"):
                    d_osm = "Sector " + via_osm[0][len("__lugar__"):]
                elif via_osm[0]:
                    d_osm = _nom(via_osm[0], v1) + (f", km {km}" if tipo == "via_km" and km else "")
                elif tipo in ("ruta_km", "ruta"):
                    d_osm = FMT_RUTA_KM.format(r=ruta, k=km) if km else FMT_RUTA.format(r=ruta)
                _sal.append((round(lat, 6), round(lon, 6), metodo, PRECISION_M.get(metodo), d_osm))
            else:
                _sal.append((None, None, "sin coordenadas", None, None))
            if (_i + 1) % 100_000 == 0:
                glog.info("  %s consultas resueltas (%s)", miles(_i + 1), duracion(time.time() - _tq))
        _q[["lat", "lon", "geo_metodo", "geo_precision_m", "direccion_osm"]] = pd.DataFrame(_sal, index=_q.index)
        glog.info("Consultas procesadas en %s", duracion(time.time() - _tq))

        _res = _dir.fillna({c: SENT for c in _claves}).merge(_q.drop(columns="n"), on=_claves, how="left")
        _res = _res.replace({SENT: None})
        _res["geo_metodo"] = _res["geo_metodo"].fillna("sin direccion")

        # --- Diagnosticos
        _tot = len(_res)
        _con = _res["lat"].notna()
        glog.info("Filas con coordenadas: %s de %s (%.1f %%)", miles(int(_con.sum())), miles(_tot), _con.mean() * 100)
        _t = (pd.crosstab(_res["tipo_direccion"].fillna("sin_dato"), _res["geo_metodo"]))
        glog.info("Metodo por tipo de direccion (filas):\n%s", _t.to_string())
        _pa = _res.groupby("anio_archivo")["lat"].apply(lambda s: s.notna().mean() * 100).round(1)
        glog.info("%% de filas con coordenadas por anio: %s", " | ".join(f"{a}: {v}" for a, v in _pa.items()))
        _res["cod_region"] = _res["cod_comuna"].astype(str).str.zfill(5).str[:2]
        _pr = _res.groupby("cod_region")["lat"].apply(lambda s: s.notna().mean() * 100).round(1)
        glog.info("%% de filas con coordenadas por region: %s",
                  " | ".join(f"{REGIONES.get(r, r)}: {v}" for r, v in _pr.items()))
        _prec = _res["geo_precision_m"].dropna().astype(float)
        if len(_prec):
            glog.info("Precision estimada: %.1f %% a 15 m o menos | %.1f %% a 60 m o menos | %.1f %% a 300 m o menos",
                      (_prec <= 15).mean() * 100, (_prec <= 60).mean() * 100, (_prec <= 300).mean() * 100)
        _sp = pd.Series(_geo.sin_pareja).sort_values(ascending=False)
        if len(_sp):
            glog.info("Calles sin pareja en OSM mas frecuentes (comuna | clave | filas):\n%s", "\n".join(
                f"  {TABLA_CUT.get(c, (c,))[0]:<22} {k:<40} {miles(v):>7}" for (c, k), v in _sp.head(40).items()))
            _sp.rename("filas").rename_axis(["cod_comuna", "clave"]).reset_index().to_csv(
                GEO_DIR / "calles_sin_pareja.csv", index=False, sep=CSV_SEP, encoding=ENCODING)
            # Sugerencias para completar a mano: las 500 calles sin pareja con mas filas y hasta 3 nombres OSM
            # parecidos de la comuna o sus vecinas (similitud por conjunto de palabras, sin umbral)
            from rapidfuzz import process as _rp, fuzz as _rf
            _filas_s = []
            for (c, k), v in _sp.head(500).items():
                cands = []
                for cv in _geo._comunas(c):
                    nombres_c = _geo._nombres.get(cv, [])
                    # promedio de similitud por conjunto y por orden de palabras: castiga nombres incompletos
                    for nm, sc, _ in _rp.extract(k, nombres_c, limit=3,
                                                 scorer=lambda a_, b_, **kw: (_rf.token_set_ratio(a_, b_) + _rf.token_sort_ratio(a_, b_)) / 2):
                        disp = _idx.get("nombre_osm_c", {}).get(cv, {}).get(nm) or _idx["nombre_osm"].get(nm, nm)
                        sc = sc + (3 if cv == c else 0)   # a igual similitud, primero la comuna del accidente
                        cands.append((min(sc, 100), disp + ("" if cv == c else f" [{TABLA_CUT.get(cv, (cv,))[0]}]")))
                cands = sorted(set(cands), key=lambda x: -x[0])[:3]
                fila = {"cod_comuna": c, "comuna": TABLA_CUT.get(c, (c,))[0], "nombre_dato": _geo.ejemplo.get((c, k), k),
                        "filas": int(v), "nombre_osm": "", "lat": "", "lon": "", "precision_m": "", "nota": ""}
                for j, (sc, nm) in enumerate(cands, 1):
                    fila[f"sugerencia_{j}"], fila[f"similitud_{j}"] = nm, round(sc)
                _filas_s.append(fila)
            escribir_csv_seguro(pd.DataFrame(_filas_s), GEO_DIR / "alias_sugeridos.csv")
            glog.info("Sugerencias para alias manuales: %s (%d calles, %s filas en total)",
                      GEO_DIR / "alias_sugeridos.csv", len(_filas_s), miles(int(_sp.head(500).sum())))
        if _geo.alias_invalidos:
            glog.warning("Alias manuales cuyo nombre OSM no existe en la comuna ni sus vecinas (revisar):\n%s", "\n".join(
                f"  {TABLA_CUT.get(c, (c,))[0] if c else '(todas)':<22} {k} -> {m}  ({miles(v)} filas)"
                for (c, k, m), v in _geo.alias_invalidos.most_common(30)))
        _vt = pd.Series(_geo.vias_tipo).sort_values(ascending=False)
        glog.info("Como se encontro cada nombre de calle en OSM (filas de la primera consulta): %s",
                  " | ".join(f"{k}: {miles(v)}" for k, v in _vt.items()))
        _ap = pd.Series(_geo.aproximados).sort_values(ascending=False)
        if len(_ap):
            _ap.rename("filas").rename_axis(["cod_comuna", "clave_dato", "clave_osm", "forma"]).reset_index().to_csv(
                GEO_DIR / "nombres_aproximados.csv", index=False, sep=CSV_SEP, encoding=ENCODING)
            for _forma in ("alias manual", "aproximado", "contenido", "iniciales", "sin direccional", "nucleo", "sin espacios",
                           "alias"):
                _sel = _ap[[f.split(" (")[0] == _forma for (_, _, _, f) in _ap.index]]
                if not len(_sel):
                    continue
                _r = random.Random(SEMILLA_MUESTRA)
                _idx_m = _r.sample(list(_sel.index), min(12, len(_sel)))
                glog.info("Nombres aceptados por '%s' (%s nombres; 12 al azar con semilla %d; comuna | dato -> OSM | filas):\n%s",
                          _forma, miles(len(_sel)), SEMILLA_MUESTRA, "\n".join(
                              f"  {TABLA_CUT.get(c, (c,))[0]:<22} {k} -> {o}  ({miles(_sel[(c, k, o, f)])})"
                              for (c, k, o, f) in _idx_m))
        # Muestra aleatoria de resultados por metodo (para verificar en un mapa)
        _con_coord = _res[_res["lat"].notna()]
        glog.info("MUESTRA ALEATORIA de coordenadas por metodo (semilla %d):", SEMILLA_MUESTRA)
        for _met, _g in _con_coord.groupby("geo_metodo"):
            glog.info("  %s (%s filas):", _met, miles(len(_g)))
            for _f in _g.sample(n=min(4, len(_g)), random_state=SEMILLA_MUESTRA).itertuples(index=False):
                cut = str(_f.cod_comuna).zfill(5)
                glog.info("    [%s | %s] %s  ->  %s  (%.6f, %.6f)", _f.anio_archivo, TABLA_CUT.get(cut, (cut,))[0],
                          _f.direccion, _f.direccion_osm, float(_f.lat), float(_f.lon))

        # --- Escritura
        _out = ["anio_archivo", "id_accidente", "lat", "lon", "geo_metodo", "geo_precision_m", "direccion_osm"]
        _res[_out + ["cod_comuna", "direccion", "tipo_direccion"]].to_csv(
            GEO_DIR / "coordenadas.csv", index=False, sep=CSV_SEP, encoding=ENCODING)
        _ruta_sin = UNION_DIR / "siniestros.csv"
        if _ruta_sin.exists():
            _sin = leer_csv(_ruta_sin).drop(columns=_out[2:], errors="ignore")
            _sin = _sin.merge(_res[_out].astype({"anio_archivo": str, "id_accidente": str}),
                              on=["anio_archivo", "id_accidente"], how="left")
            escribir_csv_seguro(_sin, _ruta_sin)
            log.info("Escrito: union\\siniestros.csv con coordenadas (%.1f %% con lat/lon)", _sin["lat"].notna().mean() * 100)
            glog.info("Escrito: %s (%s filas)", _ruta_sin, miles(len(_sin)))
        glog.info("Archivos de depuracion en %s: coordenadas.csv, calles_sin_pareja.csv, nombres_aproximados.csv", GEO_DIR)
        _ok = 1
    except Exception as exc:
        log.error("Fallo en la etapa 8: %s: %s", type(exc).__name__, exc)
        glog.exception("Fallo en la etapa 8")
        _fa = 1
    glog.info("Etapa 8 terminada en %s", duracion(time.time() - _t0))
    RESUMEN_ETAPAS[8] = {"nombre": "geolocalizacion", "ok": _ok, "omitidos": 0, "fallidos": _fa, "seg": time.time() - _t0}

# ===========================================================================
# ETAPA 9: exportacion compacta para el dashboard
# ===========================================================================
# Archivos Parquet con una fila por siniestro, tipos compactos y las variables que usa el dashboard
# (incluye agregados de personas y vehiculos). Estan pensados para subirse al repositorio, porque
# Streamlit Community Cloud solo ejecuta el dashboard. La pagina web de GitHub no acepta archivos de
# mas de 25 MiB, asi que la tabla se parte en siniestros_1.parquet, siniestros_2.parquet, etc., cada
# uno bajo DASH_PARTE_MB; el dashboard lee y une todas las partes.
DASH_DIR = BASE_DIR / "dashboard" / "datos"
DASH_PARTE_MB = 23
# Grupos de vehiculos por palabra clave sobre tipo_vehiculo (minusculas, sin tildes)
GRUPOS_VEHICULO = {
    # "moto" como palabra o prefijo de motocicleta: "Patin Motorizado", "Triciclo Motorizado" y "Sin Motor" no son motos
    "moto": r"motocicleta|motoneta|bicimoto|cuatrimoto|\bmoto\b",
    # triciclos sin motor o electricos de carga se agrupan con las bicicletas; los motorizados no
    "bicicleta": r"bicicleta|triciclo(?!.*motoriz)",
    "bus": r"\bbus|taxibus|minibus|microbus",
    # "camioneta" no es camion y "Tractor" (agricola) tampoco; "Tracto-Camion" entra por "camion"
    "camion": r"camion(?!eta)|semir+emolque|rampla",
}
DIAS_SEMANA = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]

if DESDE <= 9 <= HASTA:
    _t0 = time.time()
    log.info("=== ETAPA 9: exportacion para el dashboard ===")
    _ok = _fa = 0
    try:
        if importlib.util.find_spec("pyarrow") is None:
            raise RuntimeError(f'Falta pyarrow para escribir Parquet: "{sys.executable}" -m pip install pyarrow')
        DASH_DIR.mkdir(parents=True, exist_ok=True)
        _clave = ["anio_archivo", "id_accidente"]
        _sin = leer_csv(UNION_DIR / "siniestros.csv")
        _per = pd.read_csv(UNION_DIR / "personas.csv", sep=CSV_SEP, dtype=str, encoding=ENCODING,
                           usecols=_clave + ["calidad", "resultado"])
        _veh = pd.read_csv(UNION_DIR / "vehiculos.csv", sep=CSV_SEP, dtype=str, encoding=ENCODING,
                           usecols=_clave + ["tipo_vehiculo"])
        log.info("Leidos: siniestros %s | personas %s | vehiculos %s", miles(len(_sin)), miles(len(_per)), miles(len(_veh)))

        # Agregados por siniestro
        _peaton = _per["calidad"].eq("Peaton")
        _agp = (_per.assign(peatones=_peaton.astype("int16"),
                            peatones_muertos=(_peaton & _per["resultado"].eq("Muerto")).astype("int16"))
                .groupby(_clave)[["peatones", "peatones_muertos"]].sum())
        _tv = _veh["tipo_vehiculo"].fillna("").str.lower()
        for _g, _pat in GRUPOS_VEHICULO.items():
            _veh[_g] = _tv.str.contains(_pat, regex=True)
        _agv = _veh.groupby(_clave)[list(GRUPOS_VEHICULO)].any()
        for _g in GRUPOS_VEHICULO:
            log.info("  vehiculos del grupo '%s': %s (%s)", _g, miles(int(_veh[_g].sum())),
                     ", ".join(_veh.loc[_veh[_g], "tipo_vehiculo"].value_counts().head(12).index))
        _d = _sin.merge(_agp, on=_clave, how="left").merge(_agv, on=_clave, how="left")

        _fecha = pd.to_datetime(_d["fecha"], errors="coerce", format="%Y-%m-%d")
        _num = lambda c, t="int16": pd.to_numeric(_d[c], errors="coerce").fillna(0).astype(t)  # noqa: E731
        _dir = _d["direccion_osm"].where(_d["direccion_osm"].notna(), _d["direccion"]) if "direccion_osm" in _d else _d["direccion"]
        _out = pd.DataFrame({
            "anio": _fecha.dt.year.fillna(pd.to_numeric(_d["anio_archivo"], errors="coerce")).astype("int16"),
            "fecha": _fecha,
            "hora": pd.to_numeric(_d["hora"].str[:2], errors="coerce").astype("Int8"),
            "dia_semana": _fecha.dt.dayofweek.astype("Int8"),
            "cod_region": _d["cod_region"], "region": _d["region"],
            "cod_comuna": _d["cod_comuna"], "comuna": _d["comuna"],
            "urbano_rural": _d["urbano_rural"], "tipo_siniestro": _d["tipo_siniestro"], "causa": _d["causa"],
            "fallecidos": _num("fallecidos"), "graves": _num("graves"), "menos_graves": _num("menos_graves"),
            "leves": _num("leves"), "ilesos": _num("ilesos"),
            "peatones": _d["peatones"].fillna(0).astype("int16"),
            "peatones_muertos": _d["peatones_muertos"].fillna(0).astype("int16"),
            **{g: _d[g].fillna(False).astype(bool) for g in GRUPOS_VEHICULO},
            "direccion": _dir,
            "lat": pd.to_numeric(_d.get("lat"), errors="coerce").astype("float32"),
            "lon": pd.to_numeric(_d.get("lon"), errors="coerce").astype("float32"),
            "geo_metodo": _d.get("geo_metodo"),
            "geo_precision_m": pd.to_numeric(_d.get("geo_precision_m"), errors="coerce").astype("float32"),
        })
        for _c in ["cod_region", "region", "cod_comuna", "comuna", "urbano_rural", "tipo_siniestro", "causa", "geo_metodo"]:
            _out[_c] = _out[_c].astype("category")
        # Orden cronologico: comprime mejor y cada parte queda con un tramo de fechas continuo
        _out = _out.sort_values("fecha", kind="stable").reset_index(drop=True)
        _opts = dict(index=False, compression="zstd", compression_level=19)
        _n = 1
        while True:
            _partes = []
            for _i, _ini in enumerate(range(0, len(_out), -(-len(_out) // _n)), 1):
                _tmp = DASH_DIR / f"siniestros_{_i}.parquet.tmp"
                _out.iloc[_ini:_ini + -(-len(_out) // _n)].to_parquet(_tmp, **_opts)
                _partes.append(_tmp)
            _mb_max = max(_t.stat().st_size for _t in _partes) / 1024 / 1024
            if _mb_max <= DASH_PARTE_MB or _n >= 20:
                break
            for _t in _partes:
                _t.unlink()
            _n = int(_n * _mb_max / DASH_PARTE_MB) + 1
        # Solo cuando las partes nuevas estan completas se borran las anteriores y se renombran
        for _v in DASH_DIR.glob("siniestros*.parquet"):
            _v.unlink()
        for _t in _partes:
            os.replace(_t, _t.with_suffix(""))
        _mb = sum(_t.with_suffix("").stat().st_size for _t in _partes) / 1024 / 1024
        (log.warning if _mb_max > DASH_PARTE_MB else log.info)(
            "Escrito: %s (%d parte%s, %s filas, %d columnas, %.1f MB en total, la mayor %.1f MB%s)",
            DASH_DIR, len(_partes), "" if len(_partes) == 1 else "s", miles(len(_out)), _out.shape[1], _mb, _mb_max,
            "; supera el limite de subida por navegador de GitHub" if _mb_max > DASH_PARTE_MB else "")

        # Metadatos: periodo cubierto, meses completos del ultimo anio y fuentes
        _ult = _out["fecha"].max()
        _meta = {
            "generado": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "fecha_min": str(_out["fecha"].min().date()), "fecha_max": str(_ult.date()),
            "anio_max": int(_out["anio"].max()),
            "meses_anio_max": sorted(int(m) for m in _out.loc[_out["anio"] == _out["anio"].max(), "fecha"].dt.month.dropna().unique()),
            "filas": int(len(_out)),
            "con_coordenadas": int(_out["lat"].notna().sum()),
            "fuentes": {
                "siniestros": "Carabineros de Chile, Departamento OS2, transparencia activa",
                "coordenadas": "Datos (c) colaboradores de OpenStreetMap, licencia ODbL",
                "kilometraje": "Red vial de la Direccion de Vialidad, Ministerio de Obras Publicas",
            },
        }
        import json
        (DASH_DIR / "metadatos.json").write_text(json.dumps(_meta, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("Periodo %s a %s | %.1f %% con coordenadas", _meta["fecha_min"], _meta["fecha_max"],
                 _meta["con_coordenadas"] / max(1, _meta["filas"]) * 100)
        _ok = 1
    except Exception as exc:
        log.error("Fallo en la etapa 9: %s: %s", type(exc).__name__, exc)
        _fa = 1
    RESUMEN_ETAPAS[9] = {"nombre": "dashboard", "ok": _ok, "omitidos": 0, "fallidos": _fa, "seg": time.time() - _t0}

# ---------------------------------------------------------------------------
# Cierre
# ---------------------------------------------------------------------------
# Muestra aleatoria del consolidado de siniestros en su estado final (normalizado, con direccion y coordenadas)
if any(e in RESUMEN_ETAPAS for e in (6, 7, 8)) and (UNION_DIR / "siniestros.csv").exists():
    try:
        muestra_por_anio(leer_csv(UNION_DIR / "siniestros.csv"), "siniestros.csv (estado final)", bloque=3, ancho=60).to_csv(
            UNION_DIR / "muestra_aleatoria_siniestros.csv", index=False, sep=CSV_SEP, encoding=ENCODING)
    except Exception as exc:
        log.warning("No se pudo generar la muestra final de siniestros: %s", exc)

log.info("=== FIN DE LA CORRIDA ===")
for _n, _r in sorted(RESUMEN_ETAPAS.items()):
    log.info("Etapa %d (%s): ok=%d | omitidos=%d | fallidos=%d | %s", _n, _r["nombre"], _r["ok"],
             _r["omitidos"], _r["fallidos"], duracion(_r["seg"]))
log.info("Salida: %s | Log: %s", DATA_DIR, LOG_PATH)
if any(_r["fallidos"] for _r in RESUMEN_ETAPAS.values()):
    log.warning("Hubo archivos con fallos. Revisar script.log.")
    sys.exit(1)
