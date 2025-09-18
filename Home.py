import os
import streamlit as st
from utils.core import cargar_ubigeo_local, cargar_tabla_ubigeo_excel_bytes

st.set_page_config(page_title="ComercialTools – 2025", page_icon="🧰", layout="wide")
st.title("🧰 ComercialTools – 2025 (Streamlit)")

# -----------------------------
# Rutas de búsqueda (en orden)
# -----------------------------
UBIGEO_CANDIDATOS = [
    os.environ.get("UBIGEO_PATH", "ubigeo.xlsx"),   # 1) env var o raíz
    "data/ubigeo.xlsx",                             # 2) carpeta data/
    ".cache/ubigeo.parquet",                        # 3) parquet cache (si lo usas en el futuro)
]

# Estado inicial
st.session_state.setdefault("ubigeo_ready", False)
st.session_state.setdefault("ubigeo_df", None)

# -----------------------------------
# 1) AUTO–CARGA: Ubigeo local primero
# -----------------------------------
def _autocargar_ubigeo() -> bool:
    # Intenta .xlsx de las rutas candidatas
    for path in UBIGEO_CANDIDATOS:
        if path.endswith(".xlsx") and os.path.exists(path):
            df = cargar_ubigeo_local(path)
            if df is not None and not df.empty:
                st.session_state["ubigeo_df"] = df
                st.session_state["ubigeo_ready"] = True
                st.success(f"Ubigeo local cargado ✔  (origen: `{path}`, filas: {len(df)})")
                return True
        # (Opcional) si usas parquet de cache más adelante
        if path.endswith(".parquet") and os.path.exists(path):
            try:
                import pandas as pd
                df = pd.read_parquet(path)
                if df is not None and not df.empty:
                    st.session_state["ubigeo_df"] = df
                    st.session_state["ubigeo_ready"] = True
                    st.success(f"Ubigeo (cache) cargado ✔  (origen: `{path}`, filas: {len(df)})")
                    return True
            except Exception:
                pass
    return False

if not st.session_state["ubigeo_ready"]:
    _autocargar_ubigeo()

# -------------------------------------------------
# 2) Si falló la carga local, pedir archivo (upload)
# -------------------------------------------------
if not st.session_state["ubigeo_ready"]:
    st.warning(
        "No se encontró Ubigeo local. Sube el archivo para continuar."
        f"\nRutas probadas: {', '.join(UBIGEO_CANDIDATOS)}"
    )
    ubigeo_file = st.file_uploader("Sube Ubigeo (.xlsx/.xls)", type=["xlsx", "xls"], accept_multiple_files=False)

    # Procesa AUTOMÁTICAMENTE al subir (sin botón extra)
    if ubigeo_file is not None:
        df_up = cargar_tabla_ubigeo_excel_bytes(ubigeo_file.getvalue())
        if df_up is not None and not df_up.empty:
            st.session_state["ubigeo_df"] = df_up
            st.session_state["ubigeo_ready"] = True
            st.success("✅ Ubigeo cargado desde archivo subido.")
            st.rerun()  # recarga para habilitar páginas
        else:
            st.error("No se pudo leer el archivo. Verifica la hoja/columnas.")
else:
    st.markdown("---")
    st.markdown("### Herramientas disponibles")
    st.markdown("1) **Renombrado XML/PDF** — usa el Ubigeo cargado.")
    st.markdown("2) **Validación Confirming** — usa el Ubigeo cargado.")


