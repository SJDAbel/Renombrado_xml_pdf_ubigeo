import streamlit as st
import io, zipfile, tarfile
from utils.core import emparejar_y_reportar

# ====================== CONFIG DE SEGURIDAD ======================
MAX_DEPTH = 3                     # Profundidad máxima de compresión anidada
MAX_TOTAL_BYTES = 200 * 1024**2   # 200 MB descomprimidos (ajusta según tu realidad)

# Extensiones de contenedores soportadas (amplía/recorta si gustas)
ALLOWED_ARCHIVE_EXTS = {".zip", ".tar", ".gz", ".tgz", ".7z", ".rar"}

# Soportes opcionales
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
            f"Se superó el límite de tamaño total descomprimido ({MAX_TOTAL_BYTES/1024**2:.0f} MB)."
        )
    return new_total

def _iter_zip(data: bytes):
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            yield info.filename, zf.read(info)

def _iter_tar_like(data: bytes):
    # Soporta .tar, .tar.gz, .tgz, .gz (si es tar.gz) vía tarfile
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
    raise ValueError(f"Extensión de archivo no soportada: {ext}")

def colectar_xml_pdf_desde_adjuntos(uploads, max_depth=MAX_DEPTH):
    """
    Recorre archivos subidos (XML, PDF o comprimidos). Si son comprimidos,
    los abre recursivamente hasta max_depth y junta todos los XML/PDF encontrados.
    Devuelve (xml_files, pdf_files) donde cada elemento es {filename, content}.
    """
    xml_files, pdf_files = [], []
    total_bytes = 0

    from collections import deque
    q = deque()

    # Carga inicial: convierte cada upload en bytes
    for up in uploads:
        name = up.name
        data = up.read()
        total_bytes = _safe_add(total_bytes, len(data))
        q.append((name, data, 0))

    while q:
        name, data, depth = q.popleft()
        ext = _lower_ext(name)

        # Si es XML o PDF, agregar
        if ext == ".xml":
            xml_files.append({"filename": name, "content": data})
            continue
        if ext == ".pdf":
            pdf_files.append({"filename": name, "content": data})
            continue

        # Si es contenedor comprimido
        if _is_archive(name):
            if depth >= max_depth:
                st.warning(f"Se omitió contenido anidado en '{name}' por superar MAX_DEPTH={max_depth}.")
                continue
            try:
                for inner_name, inner_bytes in _dispatch_iter(name, data):
                    total_bytes = _safe_add(total_bytes, len(inner_bytes))
                    composed = f"{name}!/{inner_name}"
                    if _is_archive(inner_name):
                        q.append((composed, inner_bytes, depth + 1))
                    else:
                        # Reinyecta para que pase por la misma clasificación (xml/pdf)
                        q.append((composed, inner_bytes, depth))
            except Exception as e:
                st.error(f"No se pudo leer el archivo comprimido '{name}': {e}")
                continue
        # Otros tipos de archivo se ignoran silenciosamente

    return xml_files, pdf_files


# ====================== UI Y FLUJO PRINCIPAL ======================
st.title("📄 Renombrado de XML/PDF")

# Chequeo de prerequisito UBIGEO (tu lógica original)
if "ubigeo_ready" not in st.session_state or not st.session_state["ubigeo_ready"]:
    st.error("Primero carga la tabla de Ubigeo en **Home**.")
    try:
        st.switch_page("Home.py")
    except Exception:
        st.stop()
    st.stop()

df_ubi = st.session_state["ubigeo_df"]

# Reset helper (tu lógica original)
def limpiar_pagina():
    for k in [
        "ren_xml_files", "ren_pdf_files",
        "ren_resultado_zip", "ren_reporte_emparejamientos",
        "ren_reporte_errores"
    ]:
        if k in st.session_state:
            del st.session_state[k]
    st.session_state["ren_uploader_key"] = st.session_state.get("ren_uploader_key", 0) + 1
    st.rerun()

cols = st.columns([1,1,6])
with cols[0]:
    if st.button("🔄 Limpiar página", help="Reinicia la página para volver a cargar desde cero"):
        limpiar_pagina()

# Uploader extendido
uploader_key = st.session_state.get("ren_uploader_key", 0)
files = st.file_uploader(
    "Sube tus archivos XML, PDF o comprimidos",
    type=["xml", "pdf", "zip", "tar", "gz", "tgz", "7z", "rar"],
    accept_multiple_files=True,
    key=f"ren_uploader_{uploader_key}"
)

if files:
    # Extrae y clasifica todo (incluye compresiones anidadas)
    xml_files, pdf_files = colectar_xml_pdf_desde_adjuntos(files)

    st.session_state["ren_xml_files"] = [x["filename"] for x in xml_files]
    st.session_state["ren_pdf_files"] = [p["filename"] for p in pdf_files]

    st.success(f"Detectados: XML={len(xml_files)} | PDF={len(pdf_files)} (incluye contenido dentro de comprimidos)")

    # Procesa con tu core
    resultado_ordenado, excel_report_buffer, rep_emp_txt, rep_err_txt = emparejar_y_reportar(
        xml_files, pdf_files, df_ubi
    )

    st.session_state["ren_reporte_emparejamientos"] = rep_emp_txt
    st.session_state["ren_reporte_errores"] = rep_err_txt

    st.subheader("🧾 reporte_emparejamientos")
    st.code(rep_emp_txt)

    st.subheader("⚠ reporte_errores")
    st.code(rep_err_txt)

    # Empaqueta resultado final para descarga
    import zipfile as _zip
    zip_buffer = io.BytesIO()
    with _zip.ZipFile(zip_buffer, 'w', _zip.ZIP_DEFLATED) as zf:
        for name, content in resultado_ordenado:
            # Opcional: limpiar los "!/" de la ruta compuesta
            safe_name = name.replace("!/", "__")
            zf.writestr(safe_name, content)
        if excel_report_buffer is not None:
            zf.writestr("reporte_ubigeo.xlsx", excel_report_buffer.getvalue())
    zip_buffer.seek(0)

    st.download_button(
        "Descargar resultado (ZIP)",
        data=zip_buffer.getvalue(),
        file_name="Resultado_emparejamiento_xml_pdf_ubigeo.zip",
        mime="application/zip"
    )
else:
    st.info("Carga archivos (XML/PDF o comprimidos) para comenzar.")
