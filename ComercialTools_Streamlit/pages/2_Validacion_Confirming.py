
import streamlit as st
from utils.core import (
    build_id_facturas_por_ruc, validar_confirming_excel, emparejar_y_reportar
)

st.title("✅ Validación Confirming")

if "ubigeo_ready" not in st.session_state or not st.session_state["ubigeo_ready"]:
    st.error("Primero carga la tabla de Ubigeo en **Home**.")
    try:
        st.switch_page("Home.py")
    except Exception:
        st.stop()
    st.stop()

df_ubi = st.session_state["ubigeo_df"]

st.markdown("**Paso 1 – XML/PDF:** Sube **XML** y **PDF** para emparejar/renombrar (y para extraer el índice de IDs por RUC).")
files = st.file_uploader("XML y PDF (múltiples archivos)", type=["xml","pdf"], accept_multiple_files=True)

if files:
    xml_files = [{"filename":f.name, "content":f.read()} for f in files if f.name.lower().endswith(".xml")]
    pdf_files = [{"filename":f.name, "content":f.read()} for f in files if f.name.lower().endswith(".pdf")]

    resultado_ordenado, excel_report_buffer, rep_emp_txt, rep_err_txt = emparejar_y_reportar(xml_files, pdf_files, df_ubi)
    st.subheader("🧾 reporte_emparejamientos.txt")
    st.code(rep_emp_txt)
    st.subheader("⚠ reporte_errores.txt")
    st.code(rep_err_txt)

    id_facturas_por_ruc = build_id_facturas_por_ruc(xml_files)

    st.markdown("---")
    st.subheader("Sube Excel de Confirming")
    excel_file = st.file_uploader("Excel (.xlsx/.xls)", type=["xlsx","xls"], key="excel_conf")
    if excel_file:
        validado_buffer, errores_buffer, errores_txt = validar_confirming_excel(excel_file.read(), id_facturas_por_ruc=id_facturas_por_ruc)

        st.subheader("❗ Resumen de errores de validación")
        st.code(errores_txt)

        st.download_button("Descargar Plantilla VALIDADA (por moneda)", data=validado_buffer.getvalue(), file_name="Plantilla_Validada_Por_Moneda.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.download_button("Descargar Reporte de Errores", data=errores_buffer.getvalue(), file_name="Reporte_Errores_Validacion.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        import zipfile, io
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for name, content in resultado_ordenado:
                zf.writestr(name, content)
            if excel_report_buffer is not None:
                zf.writestr("reporte_ubigeo.xlsx", excel_report_buffer.getvalue())
        zip_buffer.seek(0)
        st.download_button("Descargar ZIP (ORDENADO + ubigeo)", data=zip_buffer.getvalue(), file_name="Resultado_emparejamiento_xml_pdf_ubigeo_confirming.zip", mime="application/zip")
else:
    st.info("Sube tus XML y PDFs para comenzar.")
