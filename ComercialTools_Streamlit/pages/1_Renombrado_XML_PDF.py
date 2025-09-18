
import streamlit as st
from utils.core import emparejar_y_reportar

st.title("📄 Renombrado de XML/PDF")

if "ubigeo_ready" not in st.session_state or not st.session_state["ubigeo_ready"]:
    st.error("Primero carga la tabla de Ubigeo en **Home**.")
    try:
        st.switch_page("Home.py")
    except Exception:
        st.stop()
    st.stop()

df_ubi = st.session_state["ubigeo_df"]

files = st.file_uploader("Sube tus archivos XML y PDF", type=["xml", "pdf"], accept_multiple_files=True)

if files:
    xml_files = [{"filename":f.name, "content":f.read()} for f in files if f.name.lower().endswith(".xml")]
    pdf_files = [{"filename":f.name, "content":f.read()} for f in files if f.name.lower().endswith(".pdf")]
    st.write(f"XML detectados: {len(xml_files)} | PDF detectados: {len(pdf_files)}")

    resultado_ordenado, excel_report_buffer, rep_emp_txt, rep_err_txt = emparejar_y_reportar(xml_files, pdf_files, df_ubi)

    st.subheader("🧾 reporte_emparejamientos")
    st.code(rep_emp_txt)

    st.subheader("⚠ reporte_errores")
    st.code(rep_err_txt)

    import zipfile, io
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for name, content in resultado_ordenado:
            zf.writestr(name, content)
        if excel_report_buffer is not None:
            zf.writestr("reporte_ubigeo.xlsx", excel_report_buffer.getvalue())
    zip_buffer.seek(0)

    st.download_button("Descargar resultado (ZIP)", data=zip_buffer.getvalue(), file_name="Resultado_emparejamiento_xml_pdf_ubigeo.zip", mime="application/zip")
else:
    st.info("Carga archivos XML y PDF para comenzar.")
