"""
dashboard/app.py

Dashboard de siniestros viales en Chile, a partir de los registros del Departamento OS2 de
Carabineros (2010 en adelante). Lee dashboard/datos/siniestros.parquet, que genera la etapa 9
de script.py.

Uso local:
    pip install -r dashboard/requirements.txt
    streamlit run dashboard/app.py

Streamlit Community Cloud: repositorio con dashboard/app.py como archivo principal y
dashboard/datos/ incluido en el repositorio (la carpeta data/ del pipeline no se sube).
"""
from __future__ import annotations

import json
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st

DATOS = Path(__file__).parent / "datos"
MAX_PUNTOS = 30_000          # sobre este numero el mapa muestra densidad en vez de puntos
MIN_PUNTO_CRITICO = 3        # siniestros minimos para listar una ubicacion como punto critico

st.set_page_config(page_title="Siniestros viales en Chile", page_icon=":material/traffic:", layout="wide")

# ---------------------------------------------------------------------------
# Paleta (instancia de referencia validada: azul secuencial y colores de estado)
# ---------------------------------------------------------------------------
try:
    OSCURO = st.context.theme.type == "dark"
except Exception:
    OSCURO = False
AZUL = "#3987e5" if OSCURO else "#2a78d6"
CRITICO, SERIO = "#d03b3b", "#ec835a"
TEXTO_2 = "#c3c2b7" if OSCURO else "#52514e"   # texto secundario (las etiquetas nunca usan el color de la serie)
RAMPA = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
RAMPA_TEMA = list(reversed(RAMPA)) if OSCURO else RAMPA   # en oscuro, "poco" queda cerca del fondo
DIAS = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
MESES_CORTOS = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
ETIQUETAS_MILES = "replace(datum.label, /,/g, '.')"   # separador de miles chileno en los ejes

MEDIDAS = {
    "Siniestros": ("siniestros", "siniestros"),
    "Fallecidos": ("fallecidos", "fallecidos"),
    "Lesionados graves": ("graves", "lesionados graves"),
    "Siniestros con fallecidos o graves": ("ksi", "siniestros con fallecidos o graves"),
}
GRAVEDADES = ["Todos los siniestros", "Con lesionados", "Con fallecidos o lesionados graves", "Con fallecidos"]
USUARIOS = {"Peatones": "peatones", "Motocicletas": "moto", "Bicicletas": "bicicleta", "Buses": "bus", "Camiones": "camion"}
METODOS = {
    "cruce exacto": "Cruce de dos calles en OpenStreetMap",
    "cruce exacto (variante de nombre)": "Cruce, con una variante del nombre (Rotonda, Sur, etc.)",
    "cruce exacto (nombre alternativo)": "Cruce, con otro nombre plausible de la calle",
    "cruce por cercania": "Punto más cercano entre dos calles sin nodo común",
    "numero exacto": "Número de casa registrado en OpenStreetMap",
    "numero interpolado": "Número interpolado entre vecinos de la misma vereda",
    "numero interpolado (tramo largo)": "Número interpolado en un tramo largo",
    "numero cercano": "Número de casa cercano (hasta 50 números)",
    "km en hito": "Hito kilométrico de OpenStreetMap",
    "km entre hitos": "Interpolado entre hitos kilométricos",
    "km sobre red vial MOP": "Kilometraje sobre la red vial de Vialidad (MOP)",
    "punto manual": "Punto cargado a mano (alias_calles.csv)",
    "localidad": "Centro de la localidad o sector",
    "calle en la comuna": "Punto medio de la calle en la comuna",
    "ruta en la comuna": "Punto medio de la ruta en la comuna",
    "sin coordenadas": "La dirección no se encontró en OpenStreetMap",
    "sin direccion": "El parte no trae una dirección utilizable",
}


def fmt(n, dec: int = 0) -> str:
    """Numero con separador de miles chileno (punto) y decimales con coma."""
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "-"
    return f"{n:,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def rgb(hexa: str, alfa: int = 255) -> list[int]:
    return [int(hexa[i:i + 2], 16) for i in (1, 3, 5)] + [alfa]


# ---------------------------------------------------------------------------
# Datos
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner="Cargando siniestros...")
def cargar() -> tuple[pd.DataFrame, dict]:
    df = pd.read_parquet(DATOS / "siniestros.parquet")
    df["ksi"] = ((df["fallecidos"] + df["graves"]) > 0).astype("int8")
    df["lesionados"] = (df[["fallecidos", "graves", "menos_graves", "leves"]].sum(axis=1) > 0)
    df["siniestros"] = np.int8(1)
    df["mes_inicio"] = df["fecha"].dt.to_period("M").dt.to_timestamp()
    meta = {}
    if (DATOS / "metadatos.json").exists():
        meta = json.loads((DATOS / "metadatos.json").read_text(encoding="utf-8"))
    return df, meta


if not (DATOS / "siniestros.parquet").exists():
    st.error("No se encontro dashboard/datos/siniestros.parquet. Generalo con: python script.py --desde 9")
    st.stop()

df, meta = cargar()
anio_max = int(meta.get("anio_max", df["anio"].max()))
meses_ultimo = meta.get("meses_anio_max", list(range(1, 13)))
parcial = len(meses_ultimo) < 12
texto_parcial = f"{anio_max} incluye {MESES_CORTOS[meses_ultimo[0] - 1]} a {MESES_CORTOS[meses_ultimo[-1] - 1]}" if parcial else ""

# ---------------------------------------------------------------------------
# Filtros
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Filtros")
    a_min, a_max = int(df["anio"].min()), int(df["anio"].max())
    anios = st.slider("Años", a_min, a_max, (a_min, a_max))
    regiones = (df[["cod_region", "region"]].drop_duplicates().dropna().sort_values("cod_region")["region"].astype(str).tolist())
    region = st.selectbox("Región", ["Todas"] + regiones)
    pool = df.loc[df["region"] == region, "comuna"] if region != "Todas" else df["comuna"]
    comunas = st.multiselect("Comunas", sorted(pool.dropna().astype(str).unique()), placeholder="Todas")
    zona = st.segmented_control("Zona", ["Urbano", "Rural"], selection_mode="multi", default=["Urbano", "Rural"])
    tipos = st.multiselect("Tipo de siniestro", sorted(df["tipo_siniestro"].dropna().astype(str).unique()), placeholder="Todos")
    gravedad = st.radio("Gravedad", GRAVEDADES)
    usuarios = st.multiselect("Involucra a", list(USUARIOS), placeholder="Cualquier usuario",
                              help="Siniestros con al menos un peatón, o con al menos un vehículo del tipo elegido.")
    st.divider()
    medida = st.radio("Medida de los gráficos", list(MEDIDAS))
    precision = st.select_slider("Precisión mínima de la ubicación", options=[15, 60, 300, 1000, 5000], value=60,
                                 format_func=lambda v: f"{fmt(v)} m",
                                 help="El mapa y los puntos críticos solo usan siniestros ubicados con al menos esta "
                                      "precisión estimada. 60 m deja fuera los puntos medios de calle y de ruta.")
    st.divider()
    st.caption("Datos: Carabineros de Chile (OS2). Coordenadas: © colaboradores de OpenStreetMap (ODbL) y red vial "
               "de la Dirección de Vialidad (MOP).")

m = df["anio"].between(*anios)
if region != "Todas":
    m &= df["region"] == region
if comunas:
    m &= df["comuna"].isin(comunas)
if zona:
    m &= df["urbano_rural"].isin(zona)
else:
    m &= False
if tipos:
    m &= df["tipo_siniestro"].isin(tipos)
if gravedad == GRAVEDADES[1]:
    m &= df["lesionados"]
elif gravedad == GRAVEDADES[2]:
    m &= df["ksi"] == 1
elif gravedad == GRAVEDADES[3]:
    m &= df["fallecidos"] > 0
if usuarios:
    mu = pd.Series(False, index=df.index)
    for u in usuarios:
        col = USUARIOS[u]
        mu |= (df[col] > 0) if col == "peatones" else df[col]
    m &= mu
f = df[m]
col_medida, nombre_medida = MEDIDAS[medida]

# ---------------------------------------------------------------------------
# Encabezado e indicadores
# ---------------------------------------------------------------------------
st.title("Siniestros viales en Chile")
ambito = ", ".join(comunas) if comunas else (region if region != "Todas" else "todo el país")
st.caption(f"{anios[0]} a {anios[1]} | {ambito} | Registros de Carabineros (OS2)"
           + (f" | {texto_parcial}" if parcial and anios[1] == anio_max else ""))

if f.empty:
    st.warning("Ningún siniestro cumple los filtros elegidos.")
    st.stop()

k1, k2, k3, k4, k5 = st.columns(5)
n_sin, n_fall, n_grav, n_ksi = len(f), int(f["fallecidos"].sum()), int(f["graves"].sum()), int(f["ksi"].sum())
k1.metric("Siniestros", fmt(n_sin), border=True)
k2.metric("Fallecidos", fmt(n_fall), border=True)
k3.metric("Lesionados graves", fmt(n_grav), border=True)
k4.metric("Siniestros graves", fmt(n_ksi), border=True,
          help="Siniestros con al menos un fallecido o un lesionado grave.")
k5.metric("Letalidad", fmt(n_fall / n_sin * 100, 2), border=True,
          help="Fallecidos por cada 100 siniestros.")

t_mapa, t_evol, t_hora, t_comp, t_crit, t_cal, t_acerca = st.tabs(
    ["Mapa", "Evolución", "Días y horas", "Tipos, causas y comunas", "Puntos críticos", "Calidad de la ubicación", "Acerca de"])


def leyenda_gravedad() -> None:
    items = [(CRITICO, "Con fallecidos"), (SERIO, "Con lesionados graves"), (AZUL, "Sin fallecidos ni graves")]
    html = " &nbsp;&nbsp; ".join(
        f"<span style='display:inline-block;width:10px;height:10px;border-radius:50%;background:{c};"
        f"margin-right:6px;vertical-align:middle'></span>{t}" for c, t in items)
    st.markdown(f"<div style='font-size:0.85rem'>{html}</div>", unsafe_allow_html=True)


def vista(lat: pd.Series, lon: pd.Series) -> pdk.ViewState:
    la0, la1 = lat.quantile([0.05, 0.95])
    lo0, lo1 = lon.quantile([0.05, 0.95])
    span = max(la1 - la0, (lo1 - lo0) * 0.85, 0.01)
    zoom = float(np.clip(np.log2(360 / span) - 1.2, 3.6, 15))
    return pdk.ViewState(latitude=float(lat.median()), longitude=float(lon.median()), zoom=zoom, pitch=0)


# ---------------------------------------------------------------------------
# Mapa
# ---------------------------------------------------------------------------
with t_mapa:
    dm = f[f["lat"].notna() & (f["geo_precision_m"] <= precision)]
    st.caption(f"{fmt(len(dm))} siniestros en el mapa ({fmt(len(dm) / len(f) * 100, 1)} % de los filtrados). "
               f"El resto no tiene coordenadas o su ubicación es menos precisa que {fmt(precision)} m.")
    if dm.empty:
        st.info("No hay siniestros con coordenadas para estos filtros y esta precisión.")
    elif len(dm) <= MAX_PUNTOS:
        pts = dm.assign(
            gravedad=np.select([dm["fallecidos"] > 0, dm["graves"] > 0], [2, 1], 0),
            fecha_txt=dm["fecha"].dt.strftime("%d-%m-%Y"),
            hora_txt=dm["hora"].astype("Int64").astype(str).str.zfill(2) + " h",
            precision_txt=dm["geo_precision_m"].map(lambda v: f"{fmt(v)} m"),
        ).sort_values("gravedad")
        pts["color"] = pts["gravedad"].map({2: rgb(CRITICO, 230), 1: rgb(SERIO, 220), 0: rgb(AZUL, 150)})
        cols = ["lon", "lat", "color", "direccion", "comuna", "fecha_txt", "hora_txt", "tipo_siniestro",
                "fallecidos", "graves", "geo_metodo", "precision_txt"]
        datos = pts[cols].astype({c: str for c in ["direccion", "comuna", "tipo_siniestro", "geo_metodo"]})
        capa = pdk.Layer("ScatterplotLayer", data=datos, get_position=["lon", "lat"], get_fill_color="color",
                         get_radius=25, radius_min_pixels=2.5, radius_max_pixels=14, stroked=True,
                         get_line_color=[255, 255, 255, 120] if not OSCURO else [26, 26, 25, 160],
                         line_width_min_pixels=0.5, pickable=True)
        tooltip = {"html": "<b>{direccion}</b><br/>{comuna} | {fecha_txt} | {hora_txt}<br/>{tipo_siniestro}: "
                           "{fallecidos} fallecidos, {graves} graves<br/>Ubicación: {geo_metodo} ({precision_txt})",
                   "style": {"fontSize": "12px"}}
        st.pydeck_chart(pdk.Deck(layers=[capa], initial_view_state=vista(dm["lat"], dm["lon"]), tooltip=tooltip,
                                 map_style=None), height=620)
        leyenda_gravedad()
    else:
        span = max(dm["lat"].quantile(0.99) - dm["lat"].quantile(0.01), 0.05)
        celda = max(0.0015, span / 400)
        g = (dm.assign(gy=(dm["lat"] / celda).round(), gx=(dm["lon"] / celda).round())
             .groupby(["gy", "gx"]).agg(n=("lat", "size"), lat=("lat", "mean"), lon=("lon", "mean")).reset_index())
        capa = pdk.Layer("HeatmapLayer", data=g[["lon", "lat", "n"]], get_position=["lon", "lat"], get_weight="n",
                         radius_pixels=35, intensity=1, threshold=0.03, aggregation="SUM",
                         color_range=[rgb(c)[:3] for c in RAMPA_TEMA[1:]])
        st.pydeck_chart(pdk.Deck(layers=[capa], initial_view_state=vista(dm["lat"], dm["lon"]), map_style=None),
                        height=620)
        st.caption(f"Densidad de siniestros (más oscuro = más siniestros). Con {fmt(MAX_PUNTOS)} siniestros o menos "
                   "el mapa muestra cada siniestro: elige una región o comuna, o acota los filtros.")

# ---------------------------------------------------------------------------
# Evolucion
# ---------------------------------------------------------------------------
def eje_y(titulo: str = "") -> alt.Y:
    return alt.Y("valor:Q", title=titulo or None, axis=alt.Axis(labelExpr=ETIQUETAS_MILES, tickCount=5))


with t_evol:
    anual = f.groupby("anio")[col_medida].sum().reset_index(name="valor")
    anual["valor_txt"] = anual["valor"].map(fmt)
    anual["estado"] = np.where(parcial & (anual["anio"] == anio_max), f"Parcial ({texto_parcial.split('incluye ')[-1]})", "Año completo")
    enc = dict(x=alt.X("anio:O", title=None, axis=alt.Axis(labelAngle=0)), y=eje_y(),
               tooltip=[alt.Tooltip("anio:O", title="Año"), alt.Tooltip("valor_txt:N", title=medida),
                        alt.Tooltip("estado:N", title="Periodo")])
    if anual["estado"].nunique() > 1:   # el año parcial se dibuja atenuado y con leyenda
        enc["opacity"] = alt.Opacity("estado:N", scale=alt.Scale(domain=["Año completo", anual["estado"].iloc[-1]],
                                                                 range=[1, 0.45]),
                                     legend=alt.Legend(title=None, orient="top"))
    c_anual = (alt.Chart(anual, title=f"{medida} por año")
               .mark_bar(color=AZUL, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
               .encode(**enc).properties(height=300))
    st.altair_chart(c_anual, width="stretch")

    mensual = f.groupby("mes_inicio")[col_medida].sum().reset_index(name="valor")
    mensual["valor_txt"] = mensual["valor"].map(fmt)
    mensual["mes_txt"] = mensual["mes_inicio"].dt.month.map(lambda x: MESES_CORTOS[x - 1]) + " " + mensual["mes_inicio"].dt.year.astype(str)
    sel = alt.selection_point(fields=["mes_inicio"], nearest=True, on="pointerover", empty=False, clear="pointerout")
    base = alt.Chart(mensual, title=f"{medida} por mes").encode(x=alt.X("mes_inicio:T", title=None))
    linea = base.mark_line(color=AZUL, strokeWidth=2).encode(y=eje_y())
    puntos = base.mark_circle(color=AZUL, size=70).encode(
        y="valor:Q", opacity=alt.condition(sel, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("mes_txt:N", title="Mes"), alt.Tooltip("valor_txt:N", title=medida)]).add_params(sel)
    regla = base.mark_rule(color="gray", strokeDash=[3, 3]).encode(opacity=alt.condition(sel, alt.value(0.6), alt.value(0))).transform_filter(sel)
    st.altair_chart((linea + puntos + regla).properties(height=300), width="stretch")
    if parcial and anios[1] == anio_max:
        st.caption(f"{texto_parcial}: el total de ese año no es comparable con los anteriores.")

# ---------------------------------------------------------------------------
# Dias y horas
# ---------------------------------------------------------------------------
with t_hora:
    hd = f.dropna(subset=["hora", "dia_semana"]).groupby(["dia_semana", "hora"])[col_medida].sum().reset_index(name="valor")
    hd["dia"] = hd["dia_semana"].astype(int).map(lambda d: DIAS[d])
    hd["hora_txt"] = hd["hora"].astype(int).map(lambda h: f"{h:02d}:00 a {h:02d}:59")
    hd["valor_txt"] = hd["valor"].map(fmt)
    calor = (alt.Chart(hd, title=f"{medida} por día de la semana y hora")
             .mark_rect(cornerRadius=2, stroke="white" if not OSCURO else "#1a1a19", strokeWidth=1.5)
             .encode(x=alt.X("hora:O", title="Hora del día", axis=alt.Axis(labelAngle=0)),
                     y=alt.Y("dia:N", title=None, sort=DIAS),
                     color=alt.Color("valor:Q", title=medida, scale=alt.Scale(range=RAMPA_TEMA),
                                     legend=alt.Legend(orient="bottom", labelExpr=ETIQUETAS_MILES, gradientLength=240)),
                     tooltip=[alt.Tooltip("dia:N", title="Día"), alt.Tooltip("hora_txt:N", title="Hora"),
                              alt.Tooltip("valor_txt:N", title=medida)])
             .properties(height=alt.Step(34)))
    st.altair_chart(calor, width="stretch")
    st.caption("La hora corresponde a la registrada en el parte policial. Cada celda suma todos los años filtrados.")

# ---------------------------------------------------------------------------
# Tipos, causas y comunas
# ---------------------------------------------------------------------------
def barras(datos: pd.DataFrame, campo: str, titulo: str) -> alt.Chart:
    datos = datos.copy()
    datos["valor_txt"] = datos["valor"].map(fmt)
    datos["pct"] = (datos["valor"] / max(datos["valor"].sum(), 1) * 100).map(lambda v: f"{fmt(v, 1)} %")
    base = alt.Chart(datos, title=titulo).encode(
        y=alt.Y(f"{campo}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=320)),
        x=alt.X("valor:Q", title=None, axis=alt.Axis(labelExpr=ETIQUETAS_MILES, tickCount=4)),
        tooltip=[alt.Tooltip(f"{campo}:N", title=""), alt.Tooltip("valor_txt:N", title=medida),
                 alt.Tooltip("pct:N", title="Proporción")])
    barra = base.mark_bar(color=AZUL, cornerRadiusTopRight=4, cornerRadiusBottomRight=4, height={"band": 0.72})
    texto = base.mark_text(align="left", dx=4, fontSize=11, color=TEXTO_2).encode(text="valor_txt:N")
    return (barra + texto).properties(height=alt.Step(28))


with t_comp:
    c1, c2 = st.columns(2)
    with c1:
        por_tipo = f.groupby("tipo_siniestro", observed=True)[col_medida].sum().reset_index(name="valor")
        st.altair_chart(barras(por_tipo[por_tipo["valor"] > 0], "tipo_siniestro", f"{medida} por tipo de siniestro"), width="stretch")
        usu = pd.DataFrame({"usuario": list(USUARIOS),
                            "valor": [int(f.loc[(f[c] > 0) if c == "peatones" else f[c], col_medida].sum()) for c in USUARIOS.values()]})
        st.altair_chart(barras(usu, "usuario", f"{medida} con participación de cada usuario"), width="stretch")
        st.caption("Un siniestro puede involucrar a varios tipos de usuario, por lo que estas barras no suman el total.")
    with c2:
        por_causa = (f.groupby("causa", observed=True)[col_medida].sum().sort_values(ascending=False).head(12)
                     .reset_index(name="valor"))
        st.altair_chart(barras(por_causa[por_causa["valor"] > 0], "causa", f"Principales causas ({nombre_medida})"), width="stretch")
    por_comuna = (f.groupby("comuna", observed=True)[col_medida].sum().sort_values(ascending=False).head(20)
                  .reset_index(name="valor"))
    st.altair_chart(barras(por_comuna[por_comuna["valor"] > 0], "comuna", f"Comunas con más {nombre_medida}"), width="stretch")

# ---------------------------------------------------------------------------
# Puntos criticos
# ---------------------------------------------------------------------------
with t_crit:
    prec_crit = min(precision, 300)
    st.markdown(
        f"Ubicaciones (cruce, dirección o kilómetro de ruta) con {MIN_PUNTO_CRITICO} o más siniestros, entre los "
        f"ubicados con precisión de {fmt(prec_crit)} m o mejor. Los puntos medios de calle y de ruta quedan fuera "
        "porque concentran siniestros de toda una vía en un solo punto.")
    if precision > 300:
        st.caption("La precisión elegida en los filtros supera 300 m; aquí se usa 300 m.")
    bc = f[f["lat"].notna() & (f["geo_precision_m"] <= prec_crit) & f["direccion"].notna()]
    g = (bc.groupby(["comuna", "direccion"], observed=True)
         .agg(siniestros=("siniestros", "size"), fallecidos=("fallecidos", "sum"), graves=("graves", "sum"),
              ksi=("ksi", "sum"), peatones=("peatones", lambda s: int((s > 0).sum())),
              desde=("anio", "min"), hasta=("anio", "max"), lat=("lat", "median"), lon=("lon", "median"))
         .reset_index())
    g = g[g["siniestros"] >= MIN_PUNTO_CRITICO]
    if g.empty:
        st.info("No hay ubicaciones que cumplan el criterio con estos filtros.")
    else:
        o1, o2 = st.columns([3, 1])
        orden = o1.segmented_control("Ordenar por", ["Con fallecidos o graves", "Siniestros", "Fallecidos"],
                                     default="Con fallecidos o graves", required=True)
        n_top = o2.number_input("Cantidad", 10, 500, 50, step=10)
        clave = {"Con fallecidos o graves": "ksi", "Siniestros": "siniestros", "Fallecidos": "fallecidos"}[orden]
        top = g.sort_values([clave, "siniestros", "fallecidos"], ascending=False).head(int(n_top)).reset_index(drop=True)
        top.index = top.index + 1
        st.dataframe(
            top, height=420,
            column_order=["comuna", "direccion", "siniestros", "ksi", "fallecidos", "graves", "peatones", "desde", "hasta"],
            column_config={
                "comuna": "Comuna", "direccion": st.column_config.TextColumn("Ubicación", width="large"),
                "siniestros": st.column_config.NumberColumn("Siniestros", format="%d"),
                "ksi": st.column_config.NumberColumn("Con fallecidos o graves", format="%d"),
                "fallecidos": st.column_config.NumberColumn("Fallecidos", format="%d"),
                "graves": st.column_config.NumberColumn("Graves", format="%d"),
                "peatones": st.column_config.NumberColumn("Con peatones", format="%d"),
                "desde": st.column_config.NumberColumn("Desde", format="%d"),
                "hasta": st.column_config.NumberColumn("Hasta", format="%d"),
            })
        st.download_button("Descargar tabla (CSV)", top.to_csv(index_label="ranking", sep=";").encode("utf-8-sig"),
                           file_name="puntos_criticos.csv", mime="text/csv", icon=":material/download:")
        tp = top.assign(ranking=top.index.astype(str), radio=np.sqrt(top["siniestros"]) * 35,
                        color=[rgb(CRITICO, 200) if v > 0 else rgb(AZUL, 170) for v in top["fallecidos"]])
        capa = pdk.Layer("ScatterplotLayer", data=tp[["lon", "lat", "radio", "color", "ranking", "direccion", "comuna",
                                                      "siniestros", "ksi", "fallecidos"]].astype({"direccion": str, "comuna": str}),
                         get_position=["lon", "lat"], get_radius="radio", get_fill_color="color",
                         radius_min_pixels=4, radius_max_pixels=30, stroked=True, line_width_min_pixels=1,
                         get_line_color=[255, 255, 255, 200] if not OSCURO else [26, 26, 25, 220], pickable=True)
        tt = {"html": "<b>#{ranking} {direccion}</b><br/>{comuna}<br/>{siniestros} siniestros, {ksi} con fallecidos o "
                      "graves, {fallecidos} fallecidos", "style": {"fontSize": "12px"}}
        st.pydeck_chart(pdk.Deck(layers=[capa], initial_view_state=vista(tp["lat"], tp["lon"]), tooltip=tt,
                                 map_style=None), height=480)
        st.caption("Tamaño del círculo según número de siniestros; en rojo, ubicaciones con al menos un fallecido.")

# ---------------------------------------------------------------------------
# Calidad de la ubicacion
# ---------------------------------------------------------------------------
with t_cal:
    st.markdown("Cada siniestro se ubicó a partir de la dirección del parte policial. El método indica cómo se "
                "obtuvo la coordenada y, con ello, su precisión estimada (no medida).")
    cal = (f.assign(geo_metodo=f["geo_metodo"].astype(str).replace({"nan": "sin coordenadas", "None": "sin coordenadas"}))
           .groupby("geo_metodo").agg(valor=("siniestros", "size"), precision=("geo_precision_m", "first")).reset_index())
    cal["descripcion"] = cal["geo_metodo"].map(METODOS).fillna("Sin coordenadas")
    cal["precision_txt"] = cal["precision"].map(lambda v: f"{fmt(v)} m" if pd.notna(v) else "-")
    cal = cal.sort_values("precision", na_position="last")
    cal["geo_metodo"] = (cal["geo_metodo"].str.replace("numero", "número").str.replace("cercania", "cercanía")
                         .str.replace("direccion", "dirección"))   # solo para mostrar
    ch = barras(cal.rename(columns={"geo_metodo": "metodo"}), "metodo", "Siniestros por método de ubicación")
    st.altair_chart(ch, width="stretch")
    cal["valor"] = cal["valor"].map(fmt)
    tabla = cal[["geo_metodo", "descripcion", "precision_txt", "valor"]].rename(columns={
        "geo_metodo": "Método", "descripcion": "Qué significa", "precision_txt": "Precisión estimada", "valor": "Siniestros"})
    st.dataframe(tabla, hide_index=True, column_config={"Siniestros": st.column_config.TextColumn(),
                                                        "Qué significa": st.column_config.TextColumn(width="large")})
    por_anio = (f.assign(ok=f["lat"].notna() & (f["geo_precision_m"] <= 60)).groupby("anio")["ok"].mean()
                .mul(100).reset_index(name="valor"))
    por_anio["valor_txt"] = por_anio["valor"].map(lambda v: f"{fmt(v, 1)} %")
    ch2 = (alt.Chart(por_anio, title="Siniestros ubicados con precisión de 60 m o mejor, por año")
           .mark_bar(color=AZUL, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
           .encode(x=alt.X("anio:O", title=None, axis=alt.Axis(labelAngle=0)),
                   y=alt.Y("valor:Q", title=None, scale=alt.Scale(domain=[0, 100]), axis=alt.Axis(format="~s", labelExpr="datum.label + ' %'")),
                   tooltip=[alt.Tooltip("anio:O", title="Año"), alt.Tooltip("valor_txt:N", title="Proporción")])
           .properties(height=260))
    st.altair_chart(ch2, width="stretch")

# ---------------------------------------------------------------------------
# Acerca de
# ---------------------------------------------------------------------------
with t_acerca:
    st.markdown(f"""
**Fuente de los siniestros.** Registros estadísticos del Departamento OS2 de Carabineros de Chile, publicados en
transparencia activa: un registro por siniestro, con sus personas y vehículos. Periodo {meta.get('fecha_min', '')} a
{meta.get('fecha_max', '')}{f" ({texto_parcial})" if parcial else ""}.

**Definiciones.**
- *Lesionados graves*: personas con resultado "grave" en el parte.
- *Con fallecidos o graves*: siniestros con al menos un fallecido o un lesionado grave.
- *Fallecidos por 100 siniestros*: fallecidos dividido por siniestros, por 100.
- Las cifras de fallecidos corresponden a las registradas por Carabineros en el parte; no incluyen
  necesariamente a quienes fallecen después en un centro de salud.

**Ubicación.** Las coordenadas se estimaron a partir de la dirección registrada, cruzándola con calles, cruces y
números de casa de OpenStreetMap, y con el kilometraje de la red vial de la Dirección de Vialidad (MOP) para las
rutas. La precisión es una estimación según el método, no una medición. En la pestaña *Calidad de la ubicación* se
detalla cada método.

**Atribución.** Datos de siniestros: Carabineros de Chile. Coordenadas: © colaboradores de OpenStreetMap, disponibles
bajo la licencia Open Database License (ODbL). Red vial: Dirección de Vialidad, Ministerio de Obras Públicas.

Datos generados el {meta.get('generado', '-')}.
""")