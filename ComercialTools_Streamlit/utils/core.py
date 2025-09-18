
import io, os, re, xml.etree.ElementTree as ET
from datetime import datetime

import fitz  # PyMuPDF
import pandas as pd

NS = {
    'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
    'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2'
}

def _gettext(node):
    return (node.text or '').strip() if (node is not None and node.text) else None

def _findtext(root, path):
    el = root.find(path, NS)
    return _gettext(el)

def normaliza(s):
    if s is None:
        return None
    import unicodedata
    s = str(s).strip().upper()
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return s

def limpiar_numero(s: str) -> str:
    return re.sub(r'\D', '', str(s) if s is not None else '')

# ===== UBIGEO LOADERS =====
def cargar_ubigeo_local(path="ubigeo.xlsx"):
    if os.path.exists(path):
        ext = os.path.splitext(path)[1].lower()
        try:
            if ext == ".csv":
                return pd.read_csv(path, dtype=str, encoding="utf-8-sig")
            else:
                xls = pd.ExcelFile(path)
                sheet = "Table 1" if "Table 1" in xls.sheet_names else xls.sheet_names[0]
                return pd.read_excel(xls, sheet_name=sheet, dtype=str)
        except Exception:
            return None
    return None

def cargar_tabla_ubigeo_excel_bytes(excel_bytes: bytes):
    try:
        xls = pd.ExcelFile(io.BytesIO(excel_bytes))
        sheet = 'Table 1' if 'Table 1' in xls.sheet_names else xls.sheet_names[0]
        df = pd.read_excel(xls, sheet_name=sheet, dtype=str)
        return df
    except Exception:
        return None

# ===== XML / PDF UTILS =====
def extraer_datos_xml_bytes(xml_bytes):
    def _es_id_factura(s: str) -> bool:
        if not s:
            return False
        s = s.strip()
        if s.count('-') != 1:
            return False
        return bool(re.match(r'^[A-Za-z0-9]{1,4}-[0-9A-Za-z]+$', s))

    def _pick_first_text(root_node, paths):
        for p in paths:
            val = _findtext(root_node, p)
            if val:
                return normaliza(val)
        return None

    try:
        root = ET.fromstring(xml_bytes)

        id_factura = None
        for dr in root.findall('.//cac:DocumentReference', NS):
            dtc = _findtext(dr, './cbc:DocumentTypeCode')
            if (dtc or '').strip() == '01':
                cand = _findtext(dr, './cbc:ID')
                if cand and _es_id_factura(cand):
                    id_factura = cand.strip()
                    break

        if not id_factura:
            cand = _findtext(root, './cbc:ID')
            if cand and _es_id_factura(cand):
                id_factura = cand.strip()

        if not id_factura:
            for tag in root.findall('.//cbc:ID', NS):
                t = _gettext(tag)
                if t and _es_id_factura(t):
                    id_factura = t.strip()
                    break

        serie, numero = (None, None)
        if id_factura and '-' in id_factura:
            serie, numero = id_factura.split('-', 1)

        def _find_ruc_any(root_node, xpaths):
            for xp in xpaths:
                for tag in root_node.findall(xp, NS):
                    if tag is not None and tag.attrib.get('schemeID') == '6' and tag.text:
                        val = tag.text.strip()
                        if val:
                            return val
            return None

        ruc_emisor = _find_ruc_any(root, [
            './/cac:AccountingSupplierParty//cbc:ID',
            './/cac:AccountingSupplierParty//cac:PartyLegalEntity/cbc:CompanyID',
            './/cac:SellerSupplierParty//cbc:ID',
            './/cac:SellerSupplierParty//cac:PartyLegalEntity/cbc:CompanyID',
            './/cac:SenderParty//cbc:CompanyID',
            './/cac:SenderParty//cac:PartyLegalEntity/cbc:CompanyID',
        ])
        ruc_pagador = _find_ruc_any(root, [
            './/cac:AccountingCustomerParty//cbc:ID',
            './/cac:AccountingCustomerParty//cac:PartyLegalEntity/cbc:CompanyID',
            './/cac:BuyerCustomerParty//cbc:ID',
            './/cac:BuyerCustomerParty//cac:PartyLegalEntity/cbc:CompanyID',
            './/cac:ReceiverParty//cbc:CompanyID',
            './/cac:ReceiverParty//cac:PartyLegalEntity/cbc:CompanyID',
        ])

        sup_city = _pick_first_text(root, [
            './/cac:AccountingSupplierParty//cac:PostalAddress/cbc:CityName',
            './/cac:AccountingSupplierParty//cac:PartyLegalEntity//cac:RegistrationAddress/cbc:CityName',
            './/cac:SellerSupplierParty//cac:PostalAddress/cbc:CityName',
        ])
        sup_subentity = _pick_first_text(root, [
            './/cac:AccountingSupplierParty//cac:PostalAddress/cbc:CountrySubentity',
            './/cac:AccountingSupplierParty//cac:PartyLegalEntity//cac:RegistrationAddress/cbc:CountrySubentity',
            './/cac:SellerSupplierParty//cac:PostalAddress/cbc:CountrySubentity',
        ])
        sup_district = _pick_first_text(root, [
            './/cac:AccountingSupplierParty//cac:PostalAddress/cbc:District',
            './/cac:AccountingSupplierParty//cac:PartyLegalEntity//cac:RegistrationAddress/cbc:District',
            './/cac:SellerSupplierParty//cac:PostalAddress/cbc:District',
        ])

        cus_city = _pick_first_text(root, [
            './/cac:AccountingCustomerParty//cac:PostalAddress/cbc:CityName',
            './/cac:BuyerCustomerParty//cac:PartyLegalEntity//cac:RegistrationAddress/cbc:CityName',
        ])
        cus_subentity = _pick_first_text(root, [
            './/cac:AccountingCustomerParty//cac:PostalAddress/cbc:CountrySubentity',
            './/cac:BuyerCustomerParty//cac:PartyLegalEntity//cac:RegistrationAddress/cbc:CountrySubentity',
        ])
        cus_district = _pick_first_text(root, [
            './/cac:AccountingCustomerParty//cac:PostalAddress/cbc:District',
            './/cac:BuyerCustomerParty//cac:PartyLegalEntity//cac:RegistrationAddress/cbc:District',
        ])

        sup_city      = normaliza(sup_city)
        sup_subentity = normaliza(sup_subentity)
        sup_district  = normaliza(sup_district)
        cus_city      = normaliza(cus_city)
        cus_subentity = normaliza(cus_subentity)
        cus_district  = normaliza(cus_district)

        return (
            id_factura, ruc_emisor, ruc_pagador, serie, numero,
            sup_city, sup_subentity, sup_district,
            cus_city, cus_subentity, cus_district
        )
    except Exception:
        return (None, None, None, None, None, None, None, None, None, None, None)

def pdf_contiene_datos(pdf_bytes, ruc_emisor, serie, numero):
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            for page in doc:
                text = page.get_text()
                if serie and numero and (serie in text) and (numero in text) and (ruc_emisor and ruc_emisor in text):
                    return True
        return False
    except Exception:
        return False

def buscar_ubigeo(dfubi, city, subentity, district):
    if dfubi is None:
        return None
    if not city or not subentity or not district:
        return None
    mask = (dfubi.get("Departamento")==city) & (dfubi.get("Provincia")==subentity) & (dfubi.get("Distrito")==district)
    if mask.any():
        return dfubi.loc[mask, 'Ubigeo'].iloc[0]
    return None

def emparejar_y_reportar(xml_files, pdf_files, df_ubi):
    errores = []
    matches_lines = ["### RONDA 1: Emparejamientos por contenido"]
    usados_pdf_idx = set()
    resultado_ordenado = []
    reporte_rows = []
    for x in xml_files:
        extracted = extraer_datos_xml_bytes(x['content'])
        (id_xml, ruc_emisor, ruc_pagador, serie, numero,
         sup_city, sup_subentity, sup_district,
         cus_city, cus_subentity, cus_district) = extracted
        if not id_xml or not ruc_emisor:
            errores.append(f"{x['filename']} → ❌ No se pudo extraer ID o RUC")
            continue
        ubi_sup = buscar_ubigeo(df_ubi, sup_city, sup_subentity, sup_district) if df_ubi is not None else None
        ubi_cus = buscar_ubigeo(df_ubi, cus_city, cus_subentity, cus_district) if df_ubi is not None else None
        encontrado = False
        for j, p in enumerate(pdf_files):
            if j in usados_pdf_idx: 
                continue
            if pdf_contiene_datos(p['content'], ruc_emisor, serie, numero):
                matches_lines.append(f"{id_xml} → {p['filename']}")
                usados_pdf_idx.add(j)
                nombre_base = f"{(ruc_pagador or 'SINRUC')}-{id_xml}"
                resultado_ordenado.append((f"ORDENADO/{nombre_base}.xml", x['content']))
                resultado_ordenado.append((f"ORDENADO/{nombre_base}.pdf", p['content']))
                encontrado = True
                break
        if not encontrado:
            errores.append(f"{x['filename']} → ⚠ Sin PDF emparejado")
        reporte_rows.append({
            "XML_Original": x['filename'],
            "RUC_Emisor": ruc_emisor,
            "Emisor_DEP": sup_subentity, "Emisor_PROV": sup_city, "Emisor_DIST": sup_district,
            "UBIGEO_Emisor": ubi_sup,
            "RUC_Pagador": ruc_pagador,
            "Pagador_DEP": cus_subentity, "Pagador_PROV": cus_city, "Pagador_DIST": cus_district,
            "UBIGEO_Pagador": ubi_cus
        })
    reporte_emparejamientos_txt = "\n".join(matches_lines)
    reporte_errores_txt = "\n".join(errores) if errores else "✅ Todos emparejados."
    excel_report_buffer = None
    if reporte_rows:
        df_rep = pd.DataFrame(reporte_rows)
        excel_report_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_report_buffer, engine='openpyxl') as writer:
            df_rep.to_excel(writer, index=False, sheet_name="Reporte")
        excel_report_buffer.seek(0)
    return resultado_ordenado, excel_report_buffer, reporte_emparejamientos_txt, reporte_errores_txt

def build_id_facturas_por_ruc(xml_files):
    idx = {}
    for x in xml_files:
        (id_xml, ruc_emisor, _ruc_pagador, serie, numero, *_rest) = extraer_datos_xml_bytes(x['content'])
        if not (id_xml and ruc_emisor and serie and numero):
            continue
        idx.setdefault(str(ruc_emisor).strip(), set()).add(str(id_xml).strip())
    return idx

def validar_confirming_excel(file_bytes, id_facturas_por_ruc=None):
    import pandas as pd, io as _io, re
    xls = pd.ExcelFile(_io.BytesIO(file_bytes))
    required_cols = {"RUC", "Razón Social Proveedor", "Tipo Doc.", "Documento", "Vence",
                     "Moneda", "Monto Neto a Pagar", "Banco", "Cta Bancaria", "CCI", "Tipo cuenta"}
    hoja_obj = None
    for sh in xls.sheet_names:
        df_temp = pd.read_excel(xls, sheet_name=sh, dtype={"CCI": str, 'Cta Bancaria': str})
        if required_cols.issubset(set(df_temp.columns)):
            hoja_obj = sh
            break
    if hoja_obj is None:
        hoja_obj = xls.sheet_names[0]
    df = pd.read_excel(xls, sheet_name=hoja_obj, dtype={"CCI": str, 'Cta Bancaria': str})

    df["Tipo Doc."] = "FACT"

    def nz(s):
        return str(s).strip() if s is not None else ""

    def norm_upper(s):
        import unicodedata
        s = nz(s).upper()
        return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

    def validar_fecha(fecha):
        if isinstance(fecha, datetime):
            return True
        if isinstance(fecha, str):
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
                try:
                    datetime.strptime(fecha.strip(), fmt)
                    return True
                except ValueError:
                    continue
            return False
        return False

    def validar_ruc(ruc):
        s = limpiar_numero(ruc); return len(s) == 11

    def validar_razon_social(rs):
        return isinstance(rs, str) and rs.strip() != ""

    def validar_moneda(moneda):
        return isinstance(moneda, str) and norm_upper(moneda) in ["PEN","USD"]

    def validar_monto(monto):
        try:
            float(str(monto).replace(",", "").strip().lstrip("'")); return True
        except:
            return False

    def banco_tipo(cta_banco):
        b = norm_upper(cta_banco)
        if b == "BCP": return "BCP"
        if b == "BBVA": return "BBVA"
        return "OTRO"

    def validar_cta_bancaria(cta, banco_norm):
        if banco_norm == "OTRO":
            return True
        if cta is None or str(cta).strip() == "":
            return False
        cta_str = str(cta).strip().lstrip("'")
        if not cta_str.isdigit():
            return False
        if banco_norm == "BCP":
            return len(cta_str) in [13,14]
        if banco_norm == "BBVA":
            return len(cta_str) == 18
        return False

    def validar_cci(cci):
        cci_str = limpiar_numero(cci); return len(cci_str) == 20

    def validar_tipo_cuenta_para_banco(tipo, banco_norm):
        if banco_norm == "OTRO":
            return True if (tipo is None or str(tipo).strip() == "") else norm_upper(tipo) in ["CORRIENTE","AHORROS","AHORRO"]
        return isinstance(tipo, str) and norm_upper(tipo) in ["CORRIENTE","AHORROS","AHORRO"]

    def _nz(s): 
        return str(s).strip() if s is not None else ""

    def _parse_doc_numero_only(doc_str):
        s = _nz(doc_str).upper()
        m = re.match(r'^[A-Z0-9]{1,4}-(\d{1,})$', s)
        return m.group(1) if m else None

    def _split_idfact(idf):
        s = _nz(idf).upper()
        parts = s.split('-', 1)
        if len(parts) != 2:
            return None, None
        return parts[0], parts[1]

    def _best_by_numeric_suffix(numero_excel, candidatos):
        if not candidatos:
            return None
        num_x = _nz(numero_excel)
        best = None
        best_suffix = -1
        best_num_len = -1
        for cand in candidatos:
            _cs, cand_num = _split_idfact(cand)
            if not cand_num:
                continue
            max_len = min(len(cand_num), len(num_x))
            common = 0
            for i in range(1, max_len + 1):
                if cand_num[-i] == num_x[-i]:
                    common += 1
                else:
                    break
            if (common > best_suffix) or (common == best_suffix and len(cand_num) > best_num_len):
                best = cand
                best_suffix = common
                best_num_len = len(cand_num)
        return best

    if "Documento_Original" not in df.columns:
        df["Documento_Original"] = df["Documento"]

    if id_facturas_por_ruc:
        for i, row in df.iterrows():
            ruc = limpiar_numero(row.get("RUC"))
            if not validar_ruc(ruc):
                continue
            numero_x = _parse_doc_numero_only(row.get("Documento"))
            if not numero_x:
                continue
            cand_set = id_facturas_por_ruc.get(ruc, set())
            elegido = _best_by_numeric_suffix(numero_x, cand_set)
            if elegido:
                df.at[i, "Documento"] = elegido

    errores = []
    for idx, row in df.iterrows():
        fila_errores = []

        if not validar_ruc(row.get("RUC")): fila_errores.append("RUC inválido (11 dígitos)")
        if not validar_razon_social(row.get("Razón Social Proveedor")): fila_errores.append("Razón Social vacía")

        doc = nz(row.get("Documento"))
        if not re.match(r'^[A-Z0-9]{1,4}-\d{1,}$', doc, flags=re.IGNORECASE):
            fila_errores.append("Documento inválido (SERIE-NUMERO)")

        if not validar_fecha(row.get("Vence")): fila_errores.append("Fecha inválida")
        if not validar_moneda(row.get("Moneda")): fila_errores.append("Moneda inválida (PEN/USD)")
        if not validar_monto(row.get("Monto Neto a Pagar")): fila_errores.append("Monto inválido")

        banco_norm = banco_tipo(row.get("Banco"))
        if banco_norm in ("BCP","BBVA"):
            if not isinstance(row.get("Banco"), str) or row.get("Banco").strip()=="":
                fila_errores.append("Banco vacío")

        if not validar_cta_bancaria(row.get("Cta Bancaria"), banco_norm):
            if banco_norm in ("BCP","BBVA"):
                fila_errores.append("Cuenta inválida (BCP 13-14 díg., BBVA 18 díg.)")

        if not validar_cci(row.get("CCI")):
            fila_errores.append("CCI inválido (20 dígitos)")

        if not validar_tipo_cuenta_para_banco(row.get("Tipo cuenta"), banco_norm):
            if banco_norm in ("BCP","BBVA"):
                fila_errores.append("Tipo de cuenta inválido (corriente/ahorros)")
            else:
                fila_errores.append("Tipo de cuenta inválido (solo vacío o corriente/ahorros)")

        if id_facturas_por_ruc and validar_ruc(row.get("RUC")) and doc:
            ruc_key = limpiar_numero(row.get("RUC"))
            candidatos = id_facturas_por_ruc.get(ruc_key, set())
            if candidatos and doc not in candidatos:
                fila_errores.append("Documento no coincide con XML para el RUC")

        if fila_errores:
            errores.append({"Fila Excel": idx + 2, "Errores": "; ".join(fila_errores)})

    def fila_valida(row):
        return not any(e["Fila Excel"] == row.name + 2 for e in errores)

    mask_valid = df.apply(fila_valida, axis=1)
    df_validado = df[mask_valid].copy()

    validado_buffer = _io.BytesIO()
    with pd.ExcelWriter(validado_buffer, engine='openpyxl') as writer:
        if df_validado.empty:
            pd.DataFrame(columns=df.columns).to_excel(writer, sheet_name="SIN_VALIDOS", index=False)
        else:
            for moneda in df_validado["Moneda"].dropna().astype(str).str.upper().unique():
                df_moneda = df_validado[df_validado["Moneda"].astype(str).str.upper() == moneda]
                if not df_moneda.empty:
                    df_moneda.to_excel(writer, sheet_name=moneda, index=False)
    validado_buffer.seek(0)

    errores_buffer = _io.BytesIO()
    pd.DataFrame(errores).to_excel(errores_buffer, index=False, engine='openpyxl')
    errores_buffer.seek(0)

    errores_txt = "### REPORTE_ERRORES_VALIDACION\n" + "\n".join(
        [f"Fila {e['Fila Excel']}: {e['Errores']}" for e in errores]
    ) if errores else "### REPORTE_ERRORES_VALIDACION\n✅ Sin errores."

    return validado_buffer, errores_buffer, errores_txt
