import os
import streamlit as st
from utils.core import cargar_ubigeo_local, cargar_tabla_ubigeo_excel_bytes

st.set_page_config(page_title="ComercialTools – 2025", page_icon="🧰", layout="wide")
st.title("🧰 ComercialTools – 2025 (Streamlit)")

# -----------------------------
# Config: ruta local prioritaria
# -----------------------------
# Por defecto busca en ./ubigeo.xlsx (raíz del repo/servidor).
# Si quieres usar otra ruta, exporta la variable de entorno UBIGEO_PATH.
UBIGEO_PATH = os.environ.get("UBIGEO_PATH", "ubigeo.xlsx")

# Estado inicial
if "ubigeo_ready" not in st.session_state:
    st.session_state["ubigeo_ready"] = False
if "ubigeo_df" not in st.session_state:
    st.session_state["ubigeo_df"] = None

# -----------------------------------
# 1) AUTO–CARGA: Ubigeo local primero
# -----------------------------------
def _cargar_ubigeo_local_prioritario() -> bool:
    """Intenta cargar el Ubigeo local (predeterminado). True si quedó listo."""
    df = cargar_ubigeo_local(UBIGEO_PATH)
    if df is not None and not df.empty:
        st.session_state["ubigeo_df"] = df
        st.session_state["ubigeo_ready"] = True
        return True
    return False

# Si aún no hay Ubigeo cargado, intentamos el archivo local automáticamente
if not st.session_state["ubigeo_ready"]:
    _cargar_ubigeo_local_prioritario()

# -------------------------------------------
# Mensaje de estado + opción secundaria (upload)
# -------------------------------------------
if st.session_state["ubigeo_ready"]:
    df = st.session_state["ubigeo_df"]
    st.success(f"Ubigeo local cargado ✔  (filas: {len(df)})")
else:
    st.warning(
        "No se encontró Ubigeo local. "
        f"Esperaba el archivo en: `{UBIGEO_PATH}`. "
        "Puedes subirlo manualmente abajo."
    )

st.markdown("---")
st.subheader("Opción secundaria: subir/cambiar Ubigeo manualmente")

col1, col2 = st.columns([3,1])
with col1:
    ubigeo_file = st.file_uploader("Sube Ubigeo (.xlsx/.xls)", type=["xlsx","xls"], accept_multiple_files=False)
with col2:
    if st.button("Cargar Ubigeo (manual)"):
        if ubigeo_file is None:
            st.error("Selecciona un archivo primero.")
        else:
            df_up = cargar_tabla_ubigeo_excel_bytes(ubigeo_file.getvalue())
            if df_up is not None and not df_up.empty:
                st.session_state["ubigeo_df"] = df_up
                st.session_state["ubigeo_ready"] = True
                st.success("✅ Ubigeo cargado desde archivo subido (manual).")
            else:
                st.error("No se pudo leer el archivo. Verifica la hoja/columnas.")

# ----------------------
# Guía de siguientes pasos
# ----------------------
if st.session_state["ubigeo_ready"]:
    st.markdown("---")
    st.markdown("### Herramientas")
    st.markdown("1) **Renombrado XML/PDF** — usa el Ubigeo cargado.")
    st.markdown("2) **Validación Confirming** — usa el Ubigeo cargado.")
else:
    st.info(
        "Para habilitar las páginas, coloca el archivo **ubigeo.xlsx** en la raíz del repo/servidor "
        f"(o define `UBIGEO_PATH`, actual: `{UBIGEO_PATH}`), o súbelo manualmente."
    )

