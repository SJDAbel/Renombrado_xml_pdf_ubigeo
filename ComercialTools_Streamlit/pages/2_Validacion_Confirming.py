import streamlit as st
import io, os, re, math, zipfile, tarfile
import pandas as pd
from typing import Dict, List, Tuple, Optional
from utils.core import (
    build_id_facturas_por_ruc, emparejar_y_reportar  # tu core existente
)

# ====================== CONFIG DE SEGURIDAD ======================
MAX_DEPTH = 3                     # Profundidad máxima para compresiones anidadas
MAX_TOTAL_BYTES = 200 * 1024**2   # 200 MB descomprimidos
ALLOWED_ARCHIVE_EXTS = {".zip", ".tar", ".gz", ".tgz", ".7z", ".rar"}

# Soportes opcionales 7z / rar
try:
    import py7zr
    _HAS_PY7ZR = True
except Exception:
    _HAS_PY7ZR = False

try:
    import rarfile
    _HAS_RAR = True
except Exception:
    _HAS_RAR = False


# ====================== HELPERS DE ARCHIVOS ======================
def _lower_ext(name: str) -> str:
    n = name.lower()
    if n.endswith(".tar.gz") or n.endswith(".tgz"):
        return ".tgz"
    for ext in [".xml", ".pdf", ".zip", ".tar", ".gz", ".7z", ".rar"]:
        if n.endswith(ext):
            return ext
    return ""

def _is_archive(name: str) -> bool:
    return _lower_ext(name) in ALLOWED_ARCHIVE_EXTS

def _safe_add(total_bytes: int, add: int) -> int:
    new_total = total_bytes + add
    if new_total > MAX_TOTAL_BYTES:
        raise ValueError(
            f"Se superó el límite total descomprimido ({MAX_TOTAL_BYTES/1024**2:.0f} MB)."
        )
    return new_total

def _iter_zip(data: bytes):
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            yield info.filename, zf.read(info)

def _iter_tar_like(data: bytes):
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as tf:
        for m in tf.getmembers():
            if not m.isfile():
                continue
            f = tf.extractfile(m)
            if not f:
                continue
            yield m.name, f.read()

def _iter_7z(data: bytes):
    if not _HAS_PY7ZR:
        raise RuntimeError("py7zr no está instalado. Agrega 'py7zr' a requirements.txt")
    with py7zr.SevenZipFile(io.BytesIO(data), mode='r') as z:
        for name, bio in z.readall().items():
            yield name, bio.read()

def _iter_rar(data: bytes):
    if not _HAS_RAR:
        raise RuntimeError("rarfile no está instalado. Agrega 'rarfile' a requirements.txt")
    with rarfile.RarFile(io.BytesIO(data)) as rf:
        for info in rf.infolist():
            if info.is_dir():
                continue
            with rf.open(info) as f:
                yield info.filename, f.read()

def _dispatch_iter(name: str, data: bytes):
    ext = _lower_ext(name)
    if ext == ".zip":
        return _iter_zip(data)
    if ext in (".tar", ".gz", ".tgz"):
        return _iter_tar_like(data)
    if ext == ".7z":
        return _iter_7z(data)
    if ext == ".rar":
        return _iter_rar(data)
    raise ValueError(f"Extensión no soportada: {ext}")

def _basename_inside(name: str) -> str:
    """ Devuelve el filename base aunque provenga de un comprimido: 'a.zip!/b/c.xml' -> 'c.xml' """
    inner = name.split("!/")[-1]
    return os.path.basename(inner)

def colectar_xml_pdf_desde_adjuntos(uploads, max_depth=MAX_DEPTH, exclude_r_xml=True):
    """
    Recorre archivos subidos (XML, PDF o comprimidos anidados) y devuelve:
      xml_files, pdf_files  (cada item: {filename, content})
    Si exclude_r_xml=True, excluye XML cuyo nombre base empiece con 'R-'.
    """
    xml_files, pdf_files = [], []
    total_bytes = 0
    from collections import deque
    q = deque()

    for up in uploads:
        name = up.name
        data = up.read()
        total_bytes = _safe_add(total_bytes, len(data))
        q.append((name, data, 0))

    while q:
        name, data, depth = q.popleft()
        ext = _lower_ext(name)
        base = _basename_inside(name)

        if ext == ".xml":
            if exclude_r_xml and base.lower().startswith("r-"):
                # Excluye XML 'R-*'
                continue
            xml_files.append({"filename": name, "content": data})
            continue

        if ext == ".pdf":
            pdf_files.append({"filename": name, "content": data})
            continue

        if _is_archive(name):
            if depth >= max_depth:
                st.warning(f"Se omitió contenido anidado en '{name}' (profundidad > {max_depth}).")
                continue
            try:
                for inner_name, inner_bytes in _dispatch_iter(name, data):
                    total_bytes = _safe_add(total_bytes, len(inner_bytes))
                    composed = f"{name}!/{inner_name}"
                    if _is_archive(inner_name):
                        q.append((composed, inner_bytes, depth + 1))
                    else:
                        q.append((composed, inner_bytes, depth))
            except Exception as e:
                st.error(f"No se pudo leer el archivo comprimido '{name}': {e}")
                continue

    return xml_files, pdf_files


# ====================== HELPERS DE NORMALIZACIÓN ======================
RUC_REGEX = re.compile(r"(\d{11})")

def _to_safe_str(v) -> str:
    """Convierte a str de forma segura (soporta int/float/None/NaN)."""
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    s = str(v)
    if s.lower() == "nan":
        return ""
    return s.strip()

def _normalize_ruc(v) -> str:
    """
    Devuelve un RUC de 11 dígitos:
      - Si v ya es 11 dígitos → ok
      - Si v es numérico → lo convierte a entero-string
      - Si v es texto → extrae la primera secuencia de 11 dígitos
    Si no encuentra, retorna "".
    """
    s = _to_safe_str(v)
    digits = re.sub(r"\D", "", s)
    if len(digits) == 11:
        return digits
    m = RUC_REGEX.search(s)
    return m.group(1) if m else ""

def _normalize_series(v) -> Optional[str]:
    s = _to_safe_str(v)
    return s.upper() if s else None

def _normalize_corr(v) -> Optional[str]:
    """
    Devuelve correlativo sin decimales/ceros a la izquierda si es numérico.
    """
    s = _to_safe_str(v)
    if not s:
        return None
    try:
        return str(int(float(s)))
    except Exception:
        if s.isdigit():
            return str(int(s))
        return s


# ====================== INDEXADO Y VALIDACIÓN SIMPLIFICADA ======================
def _build_index_by_ruc_and_tokens(xml_files, pdf_files):
    """
    Construye índices por RUC -> listas de nombres (xml_names, pdf_names).
    Extrae tokens típicos SUNAT: tipo, serie, correl.
    """
    def parse_tokens(fname: str) -> Dict[str, Optional[str]]:
        tokens = {}
        stem = os.path.splitext(os.path.basename(fname))[0]
        parts = stem.split("-")
        # parts: [RUC, tipo, serie, correl, ...]
        if len(parts) >= 4:
            tokens["tipo"] = parts[1]
            tokens["serie"] = parts[2].upper() if parts[2] else None
            try:
                tokens["corr"] = str(int(float(parts[3])))
            except Exception:
                tokens["corr"] = parts[3]
        return tokens

    idx = {}
    for kind, flist in (("xml", xml_files), ("pdf", pdf_files)):
        for f in flist:
            base = _basename_inside(f["filename"])
            ruc = _normalize_ruc(base) or "_NORUC_"
            tk = parse_tokens(base)
            entry = {"name": base, **tk}
            idx.setdefault(ruc, {"xml": [], "pdf": []})
            idx[ruc][kind].append(entry)
    return idx

def validar_confirming_nombres_simple(
    excel_bytes: bytes,
    xml_files: List[Dict],
    pdf_files: List[Dict],
) -> Tuple[io.BytesIO, io.BytesIO, str]:
    """
    Lee el Excel y SOLO se encarga de colocar/estandarizar:
      - Nombre_XML
      - Nombre_PDF
    Estrategia:
      1) Match por RUC (obligatorio para match).
      2) Si existen columnas 'Serie' y 'Correlativo' en el Excel, se usan para afinar.
      3) Si hay múltiples candidatos → AMBIGUO (se toma el primero y se reporta).
      4) Si no hay coincidencia → SIN_MATCH.
    Devuelve:
      - validado_buffer (xlsx)
      - errores_buffer (xlsx con detalle)
      - errores_txt (resumen)
    """
    xls = pd.ExcelFile(io.BytesIO(excel_bytes))
    idx = _build_index_by_ruc_and_tokens(xml_files, pdf_files)

    sheets_out = {}
    errores_rows = []

    for sh in xls.sheet_names:
        df = xls.parse(sh)

        # Detecta RUC/Serie/Correlativo de forma tolerante
        ruc_col = None
        for c in df.columns:
            if str(c).strip().lower() in (
                "ruc", "r.u.c", "ruc cliente", "ruc cedente", "ruc_pagador", "ruc pagador"
            ):
                ruc_col = c
                break

        serie_col = next(
            (c for c in df.columns if str(c).strip().lower() in
             ("serie", "nro serie", "número de serie", "num serie", "ser", "serie doc")),
            None
        )
        corr_col = next(
            (c for c in df.columns if str(c).strip().lower() in
             ("correlativo", "nro correlativo", "número correlativo", "num correlativo",
              "nro doc", "número de documento", "cor", "correl")),
            None
        )

        # Crea columnas de salida si no existen
        if "Nombre_XML" not in df.columns: df["Nombre_XML"] = ""
        if "Nombre_PDF" not in df.columns: df["Nombre_PDF"] = ""

        # Si no hay ruc_col, detectar por fila (último recurso)
        if ruc_col is None and len(df) > 0:
            def detect_ruc_row(row):
                for v in row:
                    r = _normalize_ruc(v)
                    if r:
                        return r
                return ""
            df["_RUC_detectado_"] = df.apply(detect_ruc_row, axis=1)
            ruc_col = "_RUC_detectado_"

        for i, row in df.iterrows():
            ruc   = _normalize_ruc(row.get(ruc_col)) if ruc_col else ""
            serie = _normalize_series(row.get(serie_col)) if serie_col else None
            corr  = _normalize_corr(row.get(corr_col)) if corr_col else None

            if not ruc:
                errores_rows.append({"Hoja": sh, "Fila": i+2, "Motivo": "SIN_RUC", "Detalle": "No se encontró RUC en la fila."})
                continue

            pool = idx.get(ruc) or {"xml": [], "pdf": []}

            # --- XML ---
            xml_candidates = pool["xml"]
            if serie:
                xml_candidates = [x for x in xml_candidates if x.get("serie") == serie]
            if corr:
                xml_candidates = [x for x in xml_candidates if x.get("corr") == corr]

            if len(xml_candidates) == 1:
                df.at[i, "Nombre_XML"] = xml_candidates[0]["name"]
            elif len(xml_candidates) == 0:
                errores_rows.append({"Hoja": sh, "Fila": i+2, "Motivo": "XML_SIN_MATCH", "Detalle": f"RUC={ruc}, Serie={serie}, Corr={corr}"})
            else:
                df.at[i, "Nombre_XML"] = xml_candidates[0]["name"]
                errores_rows.append({"Hoja": sh, "Fila": i+2, "Motivo": "XML_AMBIGUO", "Detalle": f"{len(xml_candidates)} candidatos; se tomó el primero."})

            # --- PDF ---
            pdf_candidates = pool["pdf"]
            if serie:
                pdf_candidates = [x for x in pdf_candidates if x.get("serie") == serie]
            if corr:
                pdf_candidates = [x for x in pdf_candidates if x.get("corr") == corr]

            if len(pdf_candidates) == 1:
                df.at[i, "Nombre_PDF"] = pdf_candidates[0]["name"]
            elif len(pdf_candidates) == 0:
                errores_rows.append({"Hoja": sh, "Fila": i+2, "Motivo": "PDF_SIN_MATCH", "Detalle": f"RUC={ruc}, Serie={serie}, Corr={corr}"})
            else:
                df.at[i, "Nombre_PDF"] = pdf_candidates[0]["name"]
                errores_rows.append({"Hoja": sh, "Fila": i+2, "Motivo": "PDF_AMBIGUO", "Detalle": f"{len(pdf_candidates)} candidatos; se tomó el primero."})

        sheets_out[sh] = df

    # Salidas
    validado_buffer = io.BytesIO()
    with pd.ExcelWriter(validado_buffer, engine="xlsxwriter") as writer:
        for sh, df in sheets_out.items():
            df.to_excel(writer, sheet_name=sh[:31], index=False)
    validado_buffer.seek(0)

    errores_df = pd.DataFrame(errores_rows) if errores_rows else pd.DataFrame(
        [{"Hoja": "-", "Fila": "-", "Motivo": "OK", "Detalle": "Sin observaciones"}]
    )
    errores_buffer = io.BytesIO()
    with pd.ExcelWriter(errores_buffer, engine="xlsxwriter") as writer:
        errores_df.to_excel(writer, sheet_name="Errores", index=False)
    errores_buffer.seek(0)

    resumen = (
        f"VALIDACIÓN DE NOMBRES (solo Nombre_XML / Nombre_PDF)\n"
        f"- Hojas procesadas: {len(sheets_out)}\n"
        f"- Observaciones: {len(errores_rows)}\n"
        f"- Criterio: match por RUC y, si existen, por Serie + Correlativo.\n"
        f"- Nota: si hubo múltiples candidatos, se tomó el primero y se reportó como AMBIGUO.\n"
    )
    return validado_buffer, errores_buffer, resumen


# ====================== UI STREAMLIT ======================
st.title("✅ Validación Confirming")

# Gate: Ubigeo cargado
if "ubigeo_ready" not in st.session_state or not st.session_state["ubigeo_ready"]:
    st.error("Primero carga la tabla de Ubigeo en **Home**.")
    try:
        st.switch_page("Home.py")
    except Exception:
        st.stop()
    st.stop()

df_ubi = st.session_state["ubigeo_df"]

# --- Reset helper ---
def limpiar_pagina():
    for k in [
        "val_xml_files", "val_pdf_files", "val_id_idx",
        "val_rep_emp", "val_rep_err",
        "val_validado_buffer", "val_errores_buffer", "val_errores_txt"
    ]:
        st.session_state.pop(k, None)
    st.session_state["val_uploader_files_key"] = st.session_state.get("val_uploader_files_key", 0) + 1
    st.session_state["val_uploader_excel_key"] = st.session_state.get("val_uploader_excel_key", 0) + 1
    st.rerun()

# Barra de acciones
cols = st.columns([1,1,6])
with cols[0]:
    if st.button("🔄 Limpiar página", help="Reinicia para volver a cargar archivos desde cero"):
        limpiar_pagina()

st.markdown("**Paso 1 – XML/PDF:** Sube **XML/PDF o comprimidos**. "
            "Se extraerán recursivamente (XML con prefijo `R-` excluidos).")

# --- Uploader que acepta comprimidos ---
files_key = st.session_state.get("val_uploader_files_key", 0)
files = st.file_uploader(
    "XML, PDF o comprimidos (zip/tar.gz/tgz/7z/rar)",
    type=["xml","pdf","zip","tar","gz","tgz","7z","rar"],
    accept_multiple_files=True,
    key=f"val_files_{files_key}"
)

if files:
    # 1) Extrae y clasifica (incluye anidados, excluye XML con prefijo R-)
    xml_files, pdf_files = colectar_xml_pdf_desde_adjuntos(files, exclude_r_xml=True)

    st.session_state["val_xml_files"] = [x["filename"] for x in xml_files]
    st.session_state["val_pdf_files"] = [p["filename"] for p in pdf_files]

    st.success(f"Detectados: XML={len(xml_files)} | PDF={len(pdf_files)} "
               f"(incluye contenido en comprimidos; XML 'R-*' excluidos)")

    # 2) Reordenamiento/renombrado (usa tu core existente)
    resultado_ordenado, excel_report_buffer, rep_emp_txt, rep_err_txt = emparejar_y_reportar(
        xml_files, pdf_files, df_ubi
    )
    st.session_state["val_rep_emp"] = rep_emp_txt
    st.session_state["val_rep_err"] = rep_err_txt

    st.subheader("🧾 reporte_emparejamientos.txt")
    st.code(rep_emp_txt)
    st.subheader("⚠ reporte_errores.txt")
    st.code(rep_err_txt)

    # 3) Índice opcional por RUC desde XML (si lo requieres en otros pasos)
    id_facturas_por_ruc = build_id_facturas_por_ruc(xml_files)
    st.session_state["val_id_idx"] = id_facturas_por_ruc

    st.markdown("---")
    st.subheader("Paso 2 – Sube Excel de Confirming (solo completar nombres de documentos)")

    # --- Uploader de Excel ---
    excel_key = st.session_state.get("val_uploader_excel_key", 0)
    excel_file = st.file_uploader(
        "Excel (.xlsx/.xls)",
        type=["xlsx","xls"],
        key=f"val_excel_{excel_key}"
    )

    if excel_file:
        # 4) VALIDACIÓN SIMPLIFICADA: solo Nombre_XML / Nombre_PDF
        validado_buffer, errores_buffer, errores_txt = validar_confirming_nombres_simple(
            excel_file.read(),
            xml_files=xml_files,
            pdf_files=pdf_files,
        )
        st.session_state["val_validado_buffer"] = validado_buffer
        st.session_state["val_errores_buffer"] = errores_buffer
        st.session_state["val_errores_txt"] = errores_txt

        st.subheader("❗ Resumen de validación (solo nombres)")
        st.code(errores_txt)

        st.download_button(
            "Descargar Plantilla VALIDADA (Nombre_XML / Nombre_PDF)",
            data=validado_buffer.getvalue(),
            file_name="Plantilla_Validada_Nombres.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        st.download_button(
            "Descargar Reporte de Observaciones",
            data=errores_buffer.getvalue(),
            file_name="Reporte_Observaciones_Nombres.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # ZIP final (ordenado + reporte ubigeo)
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for name, content in resultado_ordenado:
                # Limpia notación de rutas anidadas
                safe_name = name.replace("!/", "__")
                zf.writestr(safe_name, content)
            if excel_report_buffer is not None:
                zf.writestr("reporte_ubigeo.xlsx", excel_report_buffer.getvalue())
        zip_buffer.seek(0)
        st.download_button(
            "Descargar ZIP (ORDENADO + ubigeo)",
            data=zip_buffer.getvalue(),
            file_name="Resultado_emparejamiento_xml_pdf_ubigeo_confirming.zip",
            mime="application/zip"
        )
else:
    st.info("Sube tus XML/PDF o comprimidos para comenzar.")
