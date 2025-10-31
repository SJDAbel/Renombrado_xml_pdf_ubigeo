# -*- coding: utf-8 -*-
# ✅ Validación Confirming – XML→EXCEL (canónico desde contenido XML)
# Requisitos (requirements.txt sugerido):
# streamlit
# pandas
# xlsxwriter
# py7zr            # opcional, sólo si usarás .7z
# rarfile          # opcional, sólo si usarás .rar
# lxml             # opcional, para XMLs pesados (aquí usamos ElementTree estándar)

import streamlit as st
import io, os, re, math, zipfile, tarfile
import pandas as pd
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple, Optional

# Tu core existente (no lo tocamos)
from utils.core import (
    build_id_facturas_por_ruc,   # si lo usas en otros pasos
    emparejar_y_reportar         # reordenamiento + reporte ubigeo
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
      - Si v es texto → extrae la primera seq de 11 dígitos
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
    """Devuelve correlativo sin decimales/ceros a la izquierda si es numérico."""
    s = _to_safe_str(v)
    if not s:
        return None
    try:
        return str(int(float(s)))
    except Exception:
        if s.isdigit():
            return str(int(s))
        return s


# ====================== PARSEO DE XML (CANÓNICO) ======================
def _safe_decode(b: bytes) -> str:
    try:
        return b.decode("utf-8", errors="ignore")
    except Exception:
        return str(b)

def _extract_xml_meta(xml_bytes: bytes) -> Dict[str, Optional[str]]:
    """
    Devuelve dict con:
      - ruc: RUC emisor (11 dígitos)
      - tipo: '01'/'03'/etc. si se encuentra
      - serie: p.ej. 'F001'
      - corr: p.ej. '00005905' (padding exacto del XML)
      - id_full: p.ej. 'F001-00005905'
    Intenta UBL estándar Perú y es tolerante a namespaces.
    """
    meta = {"ruc": None, "tipo": None, "serie": None, "corr": None, "id_full": None}
    text = _safe_decode(xml_bytes)
    try:
        it = ET.iterparse(io.BytesIO(xml_bytes))
        for _, el in it:
            if "}" in el.tag:
                el.tag = el.tag.split("}", 1)[1]  # quita namespace
        root = it.root

        # ID del comprobante (cbc:ID a nivel documento)
        id_node = root.find(".//ID")
        if id_node is not None and id_node.text:
            doc_id = id_node.text.strip()
            meta["id_full"] = doc_id
            if "-" in doc_id:
                s, c = doc_id.split("-", 1)
                meta["serie"] = s.strip().upper()
                meta["corr"]  = c.strip()
            else:
                m = re.search(r"([A-Z]{1,3}\d{1,4})\D?(\d{1,12})", doc_id.upper())
                if m:
                    meta["serie"] = m.group(1)
                    meta["corr"]  = m.group(2)

        # Tipo
        tnode = root.find(".//InvoiceTypeCode")
        if tnode is None:
            tnode = root.find(".//CreditNoteTypeCode")
        if tnode is None:
            tnode = root.find(".//DebitNoteTypeCode")
        if tnode is not None and tnode.text:
            meta["tipo"] = _to_safe_str(tnode.text)

        # RUC emisor (schemeID="6")
        ruc_node = None
        for idn in root.findall(".//ID"):
            attrs = idn.attrib or {}
            if attrs.get("schemeID", "").strip() == "6":
                r_digits = re.sub(r"\D", "", (idn.text or ""))
                if len(r_digits) == 11:
                    ruc_node = idn
                    break
        if ruc_node is not None and ruc_node.text:
            meta["ruc"] = re.sub(r"\D", "", ruc_node.text.strip())

    except Exception:
        # fallback regex
        m = re.search(r"<cbc:ID[^>]*>(.*?)</cbc:ID>", text, flags=re.IGNORECASE|re.DOTALL)
        if m:
            id_full = re.sub(r"\s+", "", m.group(1))
            meta["id_full"] = id_full
            mm = re.search(r"([A-Z]{1,3}\d{1,4})\D?(\d{1,12})", id_full, flags=re.IGNORECASE)
            if mm:
                meta["serie"] = mm.group(1).upper()
                meta["corr"]  = mm.group(2)
        m2 = re.search(r'schemeID\s*=\s*"6"[^>]*>\s*([0-9]{11})\s*<', text, flags=re.IGNORECASE)
        if m2:
            meta["ruc"] = m2.group(1)
        m3 = re.search(r"<cbc:(?:InvoiceTypeCode|CreditNoteTypeCode|DebitNoteTypeCode)[^>]*>\s*([0-9]{2})\s*<", text, flags=re.IGNORECASE)
        if m3:
            meta["tipo"] = m3.group(1)

    if meta["serie"]:
        meta["serie"] = meta["serie"].upper()
    return meta


# ====================== ÍNDICES: XML CANÓNICO + PDFs ======================
def build_index_from_xml_and_pdfs(xml_files, pdf_files):
    """
    Construye índice canónico a partir del CONTENIDO XML:
      key principal: (ruc, serie, corr)  -> { xml_name, tipo, id_full, corr_len }
    Y además:
      - map_by_sc: (serie, corr) -> set(ruc)
      - pdf_by_sc: (serie, corr) -> [pdf_name, ...]
      - pdf_by_ruc_sc: (ruc, serie, corr) -> [pdf_name, ...]
    """
    idx = {}
    map_by_sc = {}
    pdf_by_sc = {}
    pdf_by_ruc_sc = {}

    # XMLs
    for f in xml_files:
        base = _basename_inside(f["filename"])
        meta = _extract_xml_meta(f["content"])
        ruc, serie, corr, tipo, id_full = meta["ruc"], meta["serie"], meta["corr"], meta["tipo"], meta["id_full"]
        if not (ruc and serie and corr):
            continue
        key = (ruc, serie, corr)
        idx[key] = {
            "xml_name": base,
            "tipo": tipo,
            "id_full": id_full,
            "corr_len": len(corr),
        }
        map_by_sc.setdefault((serie, corr), set()).add(ruc)

    # PDFs (por nombre)
    def parse_from_name(fname: str):
        stem = os.path.splitext(os.path.basename(fname))[0]
        ruc = _normalize_ruc(stem)
        m = re.search(r"([A-Z]{1,3}\d{1,4})\D?(\d{1,12})", stem.upper())
        serie = m.group(1) if m else None
        corr  = m.group(2) if m else None
        return ruc, serie, corr

    for p in pdf_files:
        basep = _basename_inside(p["filename"])
        rucp, seriep, corrp = parse_from_name(basep)
        if seriep and corrp:
            pdf_by_sc.setdefault((seriep, corrp), []).append(basep)
            if rucp:
                pdf_by_ruc_sc.setdefault((rucp, seriep, corrp), []).append(basep)

    return idx, map_by_sc, pdf_by_sc, pdf_by_ruc_sc


# ====================== PARSEO “Documento” (Excel) ======================
def _parse_documento_excel(v) -> Tuple[Optional[str], Optional[str]]:
    """
    'Documento' puede venir 'F001-5905' o 'F001-00005905'.
    Devuelve (serie, corr_num) con:
      - serie en upper
      - corr_num SIN padding (solo dígitos) para re-padear según XML.
    """
    s = _to_safe_str(v).upper()
    if not s:
        return None, None
    s = s.replace("/", "-").replace("_", "-").replace(" ", "-")
    m = re.search(r"([A-Z]{1,3}\d{1,4})\D?(\d{1,12})", s)
    if not m:
        return None, None
    serie = m.group(1)
    corr_raw = re.sub(r"\D", "", m.group(2))
    corr_num = str(int(corr_raw)) if corr_raw else None
    return serie, corr_num


# ====================== VALIDACIÓN PRINCIPAL (XML→EXCEL) ======================
def validar_confirming_nombres_desde_xml_excel(
    excel_bytes: bytes,
    xml_files: List[Dict],
    pdf_files: List[Dict],
) -> Tuple[io.BytesIO, io.BytesIO, str]:
    """
    Flujo:
      1) Lee XML y construye índice canónico (RUC, SERIE, CORR) desde contenido del XML.
      2) Asocia PDFs por nombre (si coinciden SERIE/CORR y opcionalmente RUC).
      3) Lee Excel: por cada fila, toma 'Documento' (aunque venga sin padding),
         intenta match con índice canónico. Si Excel trae RUC, lo usa para desambiguar.
      4) Escribe: Nombre_XML, Nombre_PDF, Nombre Verificado (SERIE-CORR canon).
    """
    idx, map_by_sc, pdf_by_sc, pdf_by_ruc_sc = build_index_from_xml_and_pdfs(xml_files, pdf_files)
    xls = pd.ExcelFile(io.BytesIO(excel_bytes))
    sheets_out = {}
    errores_rows = []

    for sh in xls.sheet_names:
        df = xls.parse(sh)

        # Crear columnas de salida si no existen
        if "Nombre_XML" not in df.columns: df["Nombre_XML"] = ""
        if "Nombre_PDF" not in df.columns: df["Nombre_PDF"] = ""
        if "Nombre Verificado" not in df.columns: df["Nombre Verificado"] = ""

        # RUC (opcional para desambiguar)
        ruc_col = None
        for c in df.columns:
            if str(c).strip().lower() in (
                "ruc", "r.u.c", "ruc cliente", "ruc cedente", "ruc_pagador",
                "ruc pagador", "ruc emisor", "ruc proveedor"
            ):
                ruc_col = c
                break

        # Documento (obligatorio en este flujo)
        doc_col = next(
            (c for c in df.columns if str(c).strip().lower() in
             ("documento", "doc", "documento ref", "número doc", "numero doc", "número de documento", "nro doc")),
            None
        )
        if doc_col is None:
            errores_rows.append({"Hoja": sh, "Fila": "-", "Motivo": "SIN_COLUMNA_DOCUMENTO", "Detalle": "No se encontró columna 'Documento'."})
            sheets_out[sh] = df
            continue

        for i, row in df.iterrows():
            doc_val = row.get(doc_col)
            serie_x, corr_num_x = _parse_documento_excel(doc_val)
            ruc_x = _normalize_ruc(row.get(ruc_col)) if ruc_col else ""

            if not (serie_x and corr_num_x):
                errores_rows.append({"Hoja": sh, "Fila": i+2, "Motivo": "DOC_NO_PARSABLE", "Detalle": f"Documento='{doc_val}'"})
                continue

            # Buscar candidatos en el índice por (serie, corr) igualando corr sin ceros
            candidatos_keys = []
            for (serie_c, corr_c), rucs in map_by_sc.items():
                if serie_c != serie_x:
                    continue
                if re.sub(r"^0+", "", corr_c) == corr_num_x:
                    for ruc_c in rucs:
                        key = (ruc_c, serie_c, corr_c)
                        if key in idx:
                            candidatos_keys.append(key)

            if not candidatos_keys:
                errores_rows.append({"Hoja": sh, "Fila": i+2, "Motivo": "SIN_MATCH_XML", "Detalle": f"Documento={doc_val}, Serie={serie_x}, Corr={corr_num_x}"})
                continue

            # Desambiguar por RUC si viene en Excel
            elegido_key = None
            if ruc_x:
                for k in candidatos_keys:
                    if k[0] == ruc_x:
                        elegido_key = k
                        break
            if elegido_key is None:
                elegido_key = candidatos_keys[0]
                if len(candidatos_keys) > 1:
                    errores_rows.append({"Hoja": sh, "Fila": i+2, "Motivo": "XML_AMBIGUO", "Detalle": f"{len(candidatos_keys)} candidatos; se tomó el primero."})

            ruc_ok, serie_ok, corr_ok = elegido_key
            meta_ok = idx[elegido_key]

            # Set columnas
            df.at[i, "Nombre_XML"] = meta_ok["xml_name"]
            df.at[i, "Nombre Verificado"] = f"{serie_ok}-{corr_ok}"  # padding según XML

            # PDF por (ruc, serie, corr) o por (serie, corr)
            pdf_name = ""
            pdf_cands_ruc = pdf_by_ruc_sc.get((ruc_ok, serie_ok, corr_ok), [])
            if pdf_cands_ruc:
                pdf_name = pdf_cands_ruc[0]
                if len(pdf_cands_ruc) > 1:
                    errores_rows.append({"Hoja": sh, "Fila": i+2, "Motivo": "PDF_AMBIGUO_RUC", "Detalle": f"{len(pdf_cands_ruc)} candidatos; se tomó el primero."})
            else:
                pdf_cands = pdf_by_sc.get((serie_ok, corr_ok), [])
                if pdf_cands:
                    pdf_name = pdf_cands[0]
                    if len(pdf_cands) > 1:
                        errores_rows.append({"Hoja": sh, "Fila": i+2, "Motivo": "PDF_AMBIGUO_SC", "Detalle": f"{len(pdf_cands)} candidatos; se tomó el primero."})
                else:
                    errores_rows.append({"Hoja": sh, "Fila": i+2, "Motivo": "PDF_SIN_MATCH", "Detalle": f"RUC={ruc_ok}, Serie={serie_ok}, Corr={corr_ok}"})

            df.at[i, "Nombre_PDF"] = pdf_name

        sheets_out[sh] = df

    # --- Salidas
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
        "VALIDACIÓN DESDE XML → EXCEL\n"
        "- Fuente canónica: SERIE/CORR/RUC desde contenido del XML (no por nombre).\n"
        "- Matching Excel: se toma 'Documento', se normaliza (tolerante a ceros), y se usa RUC si existe.\n"
        "- Salida: Nombre_XML, Nombre_PDF, Nombre Verificado (SERIE-CORR con padding real del XML).\n"
        f"- Hojas procesadas: {len(sheets_out)}\n"
        f"- Observaciones: {len(errores_rows)}\n"
    )
    return validado_buffer, errores_buffer, resumen


# ====================== UI STREAMLIT ======================
st.title("✅ Validación Confirming (XML→Excel)")

# Gate: Ubigeo cargado (respetamos tu lógica existente)
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
    st.code(rep_emp_txt or "(sin contenido)")
    st.subheader("⚠ reporte_errores.txt")
    st.code(rep_err_txt or "(sin contenido)")

    # 3) Índice opcional por RUC desde XML (si lo requieres en otros pasos)
    try:
        id_facturas_por_ruc = build_id_facturas_por_ruc(xml_files)
        st.session_state["val_id_idx"] = id_facturas_por_ruc
    except Exception:
        pass

    st.markdown("---")
    st.subheader("Paso 2 – Sube Excel de Confirming")

    # --- Uploader de Excel ---
    excel_key = st.session_state.get("val_uploader_excel_key", 0)
    excel_file = st.file_uploader(
        "Excel (.xlsx/.xls)",
        type=["xlsx","xls"],
        key=f"val_excel_{excel_key}"
    )

    if excel_file:
        # 4) VALIDACIÓN: desde XML (canónico) → Excel
        validado_buffer, errores_buffer, errores_txt = validar_confirming_nombres_desde_xml_excel(
            excel_file.read(),
            xml_files=xml_files,
            pdf_files=pdf_files,
        )
        st.session_state["val_validado_buffer"] = validado_buffer
        st.session_state["val_errores_buffer"] = errores_buffer
        st.session_state["val_errores_txt"] = errores_txt

        st.subheader("❗ Resumen de validación")
        st.code(errores_txt)

        st.download_button(
            "Descargar Plantilla VALIDADA (Nombre_XML / Nombre_PDF / Nombre Verificado)",
            data=validado_buffer.getvalue(),
            file_name="Plantilla_Validada_Confirming.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        st.download_button(
            "Descargar Reporte de Observaciones",
            data=errores_buffer.getvalue(),
            file_name="Reporte_Observaciones_Confirming.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # ZIP final (archivos reordenados + reporte ubigeo)
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for name, content in resultado_ordenado:
                safe_name = name.replace("!/", "__")  # limpia rutas anidadas
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
