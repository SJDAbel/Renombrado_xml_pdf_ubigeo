
import streamlit as st
import hashlib
from utils.core import cargar_ubigeo_local, cargar_tabla_ubigeo_excel_bytes

st.set_page_config(page_title="ComercialTools – 2025", page_icon="🧰", layout="wide")

st.title("🧰 ComercialTools – 2025 (Streamlit)")

st.markdown(
    """
Antes de usar las herramientas, **carga la tabla de Ubigeo**. Se guardará en caché
y quedará disponible para todas las páginas durante la sesión.
"""
)

def _hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

@st.cache_data(show_spinner=False)
def _cache_ubigeo_from_bytes(b: bytes):
    from utils.core import cargar_tabla_ubigeo_excel_bytes
    df = cargar_tabla_ubigeo_excel_bytes(b)
    return df

if "ubigeo_ready" not in st.session_state:
    st.session_state["ubigeo_ready"] = False
if "ubigeo_df" not in st.session_state:
    st.session_state["ubigeo_df"] = None

with st.container(border=True):
    st.subheader("Paso 1 — Proveer Ubigeo")
    col1, col2 = st.columns([2,1])
    with col1:
        ubigeo_file = st.file_uploader("Sube el Ubigeo (.xlsx/.xls)", type=["xlsx","xls"], accept_multiple_files=False)
        use_local = st.checkbox("O usar archivo local `ubigeo.xlsx` si existe en el servidor", value=False)
    with col2:
        if st.button("Cargar Ubigeo"):
            df = None
            if ubigeo_file is not None:
                df = _cache_ubigeo_from_bytes(ubigeo_file.getvalue())
            elif use_local:
                df = cargar_ubigeo_local("ubigeo.xlsx")
            if df is not None and not df.empty:
                st.session_state["ubigeo_df"] = df
                st.session_state["ubigeo_ready"] = True
                st.success("✅ Ubigeo cargado y almacenado en caché para esta sesión.")
            else:
                st.session_state["ubigeo_df"] = None
                st.session_state["ubigeo_ready"] = False
                st.error("No se pudo cargar el Ubigeo. Verifica el archivo.")

if st.session_state["ubigeo_ready"]:
    df = st.session_state["ubigeo_df"]
    st.success(f"Ubigeo listo. Filas: {len(df)} — Columnas: {list(df.columns)}")
    st.markdown("---")
    st.markdown("### Herramientas")
    st.markdown("1) **Renombrado XML/PDF** (usa el Ubigeo cacheado)")
    st.markdown("2) **Validación Confirming** (usa el Ubigeo cacheado e índice de IDs por RUC)")
else:
    st.warning("Debes cargar Ubigeo para habilitar las páginas.")

st.caption("Tip: Puedes actualizar el Ubigeo en cualquier momento volviendo a esta página.")
