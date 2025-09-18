import streamlit as st
from utils.core import (
    build_id_facturas_por_ruc, validar_confirming_excel, emparejar_y_reportar
)

st.title("✅ Validación Confirming")

# --- Gate: Ubigeo cargado ---
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
    # Borra estado previo de esta página
    for k in [
        "val_xml_files", "val_pdf_files", "val_id_idx",
        "val_rep_emp", "val_rep_err",
        "val_validado_buffer", "val_errores_buffer", "val_errores_txt"
    ]:
        if k in st.session_state:
            del st.session_state[k]
    # Resetea ambos uploaders cambiando sus keys
    st.session_state["val_uploader_files_key"] = st.session_state.get("val_uploader_files_key", 0) + 1
    st.session_state["val_uploader_excel_key"] = st.session_state.get("val_uploader_excel_key", 0) + 1
    st.rerun()

# Barra de acciones
cols = st.columns([1,1,6])
with cols[0]:
    if st.button("🔄 Limpiar página", help="Reinicia para volver a cargar archivos desde cero"):
        limpiar_pagina()

st.markdown("**Paso 1 – XML/PDF:** Sube **XML** y **PDF** para emparejar/renombrar (y para extraer el índice de IDs por RUC).")

# --- Uploader de XML/PDF con key variable ---
files_key = st.session_state.get("val_uploader_files_key", 0)
files = st.file_uploader(
    "XML y PDF (múltiples archivos)",
    type=["xml","pdf"],
    accept_multiple_files=True,
    key=f"val_files_{files_key}"
)

if files:
    xml_files = [{"filename":f.name, "content":f.read()} for f in files if f.name.lower().endswith(".xml")]
    pdf_files = [{"filename":f.name, "content":f.read()} for f in files if f.name.lower().endswith(".pdf")]
    st.session_state["val_xml_files"] = [x["filename"] for x in xml_files]
    st.session_state["val_pdf_files"] = [p["filename"] for p in pdf_files]

    resultado_ordenado, excel_report_buffer, rep_emp_txt, rep_err_txt = emparejar_y_reportar(xml_files, pdf_files, df_ubi)
    st.session_state["val_rep_emp"] = rep_emp_txt
    st.session_state["val_rep_err"] = rep_err_txt

    st.subheader("🧾 reporte_emparejamientos.txt")
    st.code(rep_emp_txt)
    st.subheader("⚠ reporte_errores.txt")
    st.code(rep_err_txt)

    id_facturas_por_ruc = build_id_facturas_por_ruc(xml_files)
    st.session_state["val_id_idx"] = id_facturas_por_ruc

    st.markdown("---")
    st.subheader("Sube Excel de Confirming")

    # --- Uploader de Excel con key variable ---
    excel_key = st.session_state.get("val_uploader_excel_key", 0)
    excel_file = st.file_uploader(
        "Excel (.xlsx/.xls)",
        type=["xlsx","xls"],
        key=f"val_excel_{excel_key}"
    )

    if excel_file:
        validado_buffer, errores_buffer, errores_txt = validar_confirming_excel(
            excel_file.read(),
            id_facturas_por_ruc=id_facturas_por_ruc
        )
        st.session_state["val_validado_buffer"] = validado_buffer
        st.session_state["val_errores_buffer"] = errores_buffer
        st.session_state["val_errores_txt"] = errores_txt

        st.subheader("❗ Resumen de errores de validación")
        st.code(errores_txt)

        st.download_button(
            "Descargar Plantilla VALIDADA (por moneda)",
            data=validado_buffer.getvalue(),
            file_name="Plantilla_Validada_Por_Moneda.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        st.download_button(
            "Descargar Reporte de Errores",
            data=errores_buffer.getvalue(),
            file_name="Reporte_Errores_Validacion.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # ZIP minimal con ORDENADO y reporte ubigeo
        import zipfile, io
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
    st.info("Sube tus XML y PDFs para comenzar.")
