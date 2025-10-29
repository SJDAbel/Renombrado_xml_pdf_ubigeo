import streamlit as st
import io, os, re, zipfile, tarfile
import pandas as pd
from typing import Dict, List, Tuple, Optional
from utils.core import (
    build_id_facturas_por_ruc, emparejar_y_reportar  # usamos tu core para el zipeado final
)

# ====================== CONFIG DE SEGURIDAD ======================
MAX_DEPTH = 3                     # Profundidad máxima de compresión anidada
MAX_TOTAL_BYTES = 200 * 1024**2   # 200 MB descomprimidos
ALLOWED_ARCHIVE_EXTS = {".zip", ".tar", ".gz", ".tgz", ".7z", ".rar"}

# Soportes opcionales para 7z / rar
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


# ====================== HELPERS ARCHIVOS ======================
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
    """ Devuelve el nombre base aunque venga desde un comprimido: 'a.zip!/b/c.xml' -> 'c.xml' """
    inner = name.split("!/")[-1]
    return os.path.basename(inner)

def colectar_xml_pdf_desde_adjuntos(uploads, max_depth=MAX_DEPTH, exclude_r_xml=True):
    """
    Recorre archivos subidos (XML, PDF o comprimidos anidados) y devuelve:
      xml_files, pdf_files  con dicts {filename, content}
    Opcionalmente excluye los XML cuyo filename base empiece con 'R-'.
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


# ====================== VALIDACIÓN SIMPLIFICADA (SOLO NOMBRES) ======================
RUC_REGEX = re.compile(r"\b(\d{11})\b")

def _extract_ruc_from_filename(fname: str) -> Optional[str]:
    m = RUC_REGEX.search(fname)
    return m.group(1) if m else None

def _to_safe_name(n: str) -> str:
    return (n or "").strip()

def _normalize_series(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return str(s).strip().upper()

def _normalize_corr(c: Optional[str]) -> Optional[str]:
    if c is None:
        return None
    try:
        # Quita decimales si vienen de Excel
        return str(int(float(str(c).strip())))
    except Exception:
        return str(c).strip()

def _build_index_by_ruc_and_tokens(xml_files, pdf_files):
    """
    Construye índices por RUC -> listas de nombres (xml_names, pdf_names).
    Para matching fino, guardamos tokens por archivo (serie, correlativo si se detectan).
    """
    def parse_tokens(fname: str) -> Dict[str, str]:
        # Busca tokens comunes en nombres SUNAT: RUC-TPO-SERIE-CORR
        # ejemplo: 20123456789-01-F001-12345.xml
        tokens = {}
        parts = os.path.splitext(os.path.basename(fname))[0].split("-")
        if len(parts) >= 4:
            # asume [RUC, tipo, serie, correl]
            tokens["tipo"] = parts[1]
            tokens["serie"] = parts[2]
            tokens["corr"] = parts[3]
        return tokens

    idx = {}
    for kind, flist in (("xml", xml_files), ("pdf", pdf_files)):
        for f in flist:
            base = _basename_inside(f["filename"])
            ruc = _extract_ruc_from_filename(base) or ""
            if not ruc:
                # si no hay RUC, igual lo guardamos bajo clave especial
                ruc = "_NORUC_"
            tokens = parse_tokens(base)
            entry = {"name": base, **tokens}
            idx.setdefault(ruc, {"xml": [], "pdf": []})
            idx[ruc][kind].append(entry)
    return idx

def validar_confirming_nombres_simple(
    excel_bytes: bytes,
    xml_files: List[Dict],
    pdf_files: List[Dict],
) -> Tuple[io.BytesIO, io.BytesIO, str]:
    """
    Lee el Excel y SOLO se encarga de colocar/estandarizar las columnas:
      - Nombre_XML
      - Nombre_PDF
    Estrategia de match:
      1) Se busca por RUC (obligatorio para match).
      2) Si existen columnas 'Serie' y 'Correlativo' en el Excel, se usan para afinar coincidencia.
      3) Si hay múltiples candidatos, se marca como AMBIGUO.
      4) Si no hay coincidencia, se marca como SIN_MATCH.
    Devuelve:
      - validado_buffer (xlsx)
      - errores_buffer (xlsx con detalle de errores)
      - errores_txt (resumen)
    """
    xls = pd.ExcelFile(io.BytesIO(excel_bytes))
    idx = _build_index_by_ruc_and_tokens(xml_files, pdf_files)

    sheets_out = {}
    errores_rows = []

    for sh in xls.sheet_names:
        df = xls.parse(sh)
        # Identificar columnas clave
        # Buscamos 'RUC' y opcionalmente 'Serie'/'Correlativo' con variantes
        ruc_col = None
        for c in df.columns:
            if str(c).strip().lower() in ("ruc", "r.u.c", "ruc cliente", "ruc cedente", "ruc_pagador", "ruc pagador"):
                ruc_col = c
                break
        # Si no hay RUC, intentamos detectar uno dentro de algún campo de texto (ultimo recurso)
        if ruc_col is None and len(df) > 0:
            # crea columna virtual de ruc detectado
            def detect_ruc_row(row):
                for v in row:
                    m = RUC_REGEX.search(str(v))
                    if m:
                        return m.group(1)
                return None
            df["_RUC_detectado_"] = df.apply(detect_ruc_row, axis=1)
            ruc_col = "_RUC_detectado_"

        serie_col = next((c for c in df.columns if str(c).strip().lower() in ("serie", "nro serie", "número de serie", "num serie")), None)
        corr_col  = next((c for c in df.columns if str(c).strip().lower() in ("correlativo", "nro correlativo", "número correlativo", "num correlativo", "nro doc", "número de documento")), None)

        # Asegura columnas de salida
        if "Nombre_XML" not in df.columns: df["Nombre_XML"] = ""
        if "Nombre_PDF" not in df.columns: df["Nombre_PDF"] = ""

        for i, row in df.iterrows():
            ruc = _to_safe_name(row.get(ruc_col)) if ruc_col else ""
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
                df.at[i, "Nombre_XML"] = xml_candidates[0]["name"]  # toma el primero
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
        [{"Hoja":"-", "Fila":"-", "Motivo":"OK", "Detalle":"Sin observaciones"}]
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
st.title("✅ Validación Confirming (ZIP + Nombres)")

# Gate UBIGEO como en tus otras páginas
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

cols = st.columns([1,1,6])
with cols[0]:
    if st.button("🔄 Limpiar página", help="Reinicia para volver a cargar archivos desde cero"):
        limpiar_pagina()

st.markdown("**Paso 1 – XML/PDF:** Sube **XML/PDF o comprimidos**. Se extraerán recursivamente (R-XML excluidos).")

# --- Uploader (ahora acepta comprimidos) ---
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

    st.success(f"Detectados: XML={len(xml_files)} | PDF={len(pdf_files)} (incluye contenido en comprimidos; XML 'R-*' excluidos)")

    # 2) Opcional: renombrado/orden (usamos tu core)
    resultado_ordenado, excel_report_buffer, rep_emp_txt, rep_err_txt = emparejar_y_reportar(
        xml_files, pdf_files, df_ubi
    )
    st.session_state["val_rep_emp"] = rep_emp_txt
    st.session_state["val_rep_err"] = rep_err_txt

    st.subheader("🧾 reporte_emparejamientos.txt")
    st.code(rep_emp_txt)
    st.subheader("⚠ reporte_errores.txt")
    st.code(rep_err_txt)

    # 3) Índice por RUC desde XML (si lo quieres seguir usando en otros puntos)
    id_facturas_por_ruc = build_id_facturas_por_ruc(xml_files)
    st.session_state["val_id_idx"] = id_facturas_por_ruc

    st.markdown("---")
    st.subheader("Paso 2 – Sube Excel de Confirming (solo ajustar nombres de documentos)")

    # --- Uploader de Excel ---
    excel_key = st.session_state.get("val_uploader_excel_key", 0)
    excel_file = st.file_uploader(
        "Excel (.xlsx/.xls)",
        type=["xlsx","xls"],
        key=f"val_excel_{excel_key}"
    )

    if excel_file:
        # VALIDACIÓN SIMPLIFICADA: solo nombres
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
                zf.writestr(name, content)
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

