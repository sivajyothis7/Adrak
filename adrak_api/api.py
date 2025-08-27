import frappe
import requests
import re
from frappe.utils import getdate
from frappe.utils import today
from frappe import _
from frappe.utils.response import build_response
from frappe.utils import cint
from ksa_compliance.jinja import get_zatca_phase_1_qr_for_invoice


def parse_address_display(address_display):
    building_number = ""
    pincode = ""
    city = ""
    district = ""
    address_line1 = ""

    address_line1_match = re.search(r'Building\s+No\s+\d{4},\s*[^,]+', address_display, re.IGNORECASE)
    if address_line1_match:
        address_line1 = address_line1_match.group(0).strip()

    building_match = re.search(r'Building\s+No\s+(\d{4})', address_display, re.IGNORECASE)
    if building_match:
        building_number = building_match.group(1)

    pincode_match = re.search(r'P\.C: *(\d{5})', address_display, re.IGNORECASE)
    if pincode_match:
        pincode = pincode_match.group(1)

    district_match = re.search(r'([\w\s\-]*?Dist[\w\-]*)', address_display, re.IGNORECASE)
    if district_match:
        district = district_match.group(1).strip()

    city_match = re.search(r',([^,]+),\s*Kingdom of Saudi Arabia', address_display, re.IGNORECASE)
    if city_match:
        city = city_match.group(1).strip()

    return {
        "custom_building_number": building_number,
        "pincode": pincode,
        "city": city,
        "district": district,
        "address_line1": address_line1
    }

def address_exists_with_link(address_title, customer):
    addresses = frappe.get_all("Address", filters={"address_title": address_title}, fields=["name"])
    for addr in addresses:
        links = frappe.get_all("Dynamic Link", filters={
            "parent": addr.name,
            "parenttype": "Address",
            "link_doctype": "Customer",
            "link_name": customer
        })
        if links:
            return True
    return False


##sales Invoice

@frappe.whitelist()
def import_sales_invoices():
    frappe.set_user("Administrator")
    api_url = "https://adraklive.com/api/l/app.aspx/sales_invoices"
    headers = {"x-api-key": "05884b11-9996-4cc3-b25c-6e3f6cc6139f"}

    response = requests.get(api_url, headers=headers)
    if response.status_code != 200:
        frappe.log_error("Failed to fetch sales invoices", str(response.content))
        return

    data = response.json()
    if data.get("status") != "success":
        frappe.log_error("Invalid response from API", str(data))
        return

    for inv in data.get("payloads", []):
        try:
            invoice_name = inv.get("Naming Series")
            if not invoice_name:
                frappe.log_error("Missing Naming Series in payload", str(inv))
                continue

            if frappe.db.exists("Sales Invoice", invoice_name):
                frappe.logger().info(f"⏩ Skipping duplicate invoice: {invoice_name}")
                continue

            customer = inv.get("Customer Name")
            company = inv.get("Company Name")
            tax_id = inv.get("Tax ID")
            posting_date = getdate(inv.get("Posting Date"))
            due_date = today()
            address_title = inv.get("Customer Address")
            address_display = inv.get("Address Display")
            items = inv.get("Items", [])

            if not frappe.db.exists("Company", company):
                frappe.log_error(f"Company '{company}' not found. Invoice skipped.")
                continue

            company_doc = frappe.get_doc("Company", company)
            currency = company_doc.default_currency or "SAR"
            income_account = company_doc.default_income_account
            receivable_account = company_doc.default_receivable_account

            if not income_account or not receivable_account:
                frappe.log_error(f"Missing accounts in company: {company}. Invoice skipped.")
                continue

            if frappe.db.exists("Customer", customer):
                customer_doc = frappe.get_doc("Customer", customer)
                if tax_id and customer_doc.tax_id != tax_id:
                    customer_doc.tax_id = tax_id
                    customer_doc.save(ignore_permissions=True)
            else:
                frappe.get_doc({
                    "doctype": "Customer",
                    "customer_name": customer,
                    "customer_type": "Company",
                    "tax_id": tax_id
                }).insert(ignore_permissions=True)

            if address_title and address_display and not address_exists_with_link(address_title, customer):
                parsed = parse_address_display(address_display)
                frappe.get_doc({
                    "doctype": "Address",
                    "address_title": address_title,
                    "address_type": "Billing",
                    "address_line1": parsed["address_line1"],
                    "city": parsed["city"],
                    "pincode": parsed["pincode"],
                    "district": parsed["district"],
                    "custom_building_number": parsed["custom_building_number"],
                    "country": "Saudi Arabia",
                    "links": [{
                        "link_doctype": "Customer",
                        "link_name": customer
                    }]
                }).insert(ignore_permissions=True)

            invoice_items = []
            tax_account_map = {}

            for item in items:
                item_code = item.get("Item_Code")
                item_name = item.get("Item_Name")
                description = item.get("Description") or item_name
                qty = item.get("Qty", 1)
                rate = item.get("Rate", 0)
                uom = item.get("Uom", "Nos")
                tax_template = item.get("Item_Tax_Template")

                if uom and not frappe.db.exists("UOM", uom):
                    frappe.get_doc({
                        "doctype": "UOM",
                        "uom_name": uom
                    }).insert(ignore_permissions=True)

                if not frappe.db.exists("Item", item_code):
                    item_doc = frappe.get_doc({
                        "doctype": "Item",
                        "item_code": item_code,
                        "item_name": item_name,
                        "description": description,
                        "stock_uom": uom,
                        "is_sales_item": 1,
                        "item_group": "Services",
                        "is_stock_item": 0
                    })

                    tax_template_name = frappe.db.get_value(
                        "Item Tax Template",
                        {"name": tax_template, "company": company},
                        "name"
                    )
                    if tax_template_name:
                        item_doc.append("taxes", {
                            "item_tax_template": tax_template_name
                        })

                    item_doc.insert(ignore_permissions=True)

                invoice_items.append({
                    "item_code": item_code,
                    "item_name": item_name,
                    "description": description,
                    "qty": qty,
                    "rate": rate,
                    "uom": uom,
                    "income_account": income_account
                })

                if tax_template:
                    tax_template_name = frappe.db.get_value(
                        "Item Tax Template",
                        {"name": tax_template, "company": company},
                        "name"
                    )
                    if tax_template_name:
                        template = frappe.get_doc("Item Tax Template", tax_template_name)
                        for tmpl_row in template.taxes:
                            account_company = frappe.db.get_value("Account", tmpl_row.tax_type, "company")
                            if account_company == company:
                                key = (tmpl_row.tax_type, tmpl_row.tax_rate)
                                if key not in tax_account_map:
                                    tax_account_map[key] = {
                                        "charge_type": "On Net Total",
                                        "account_head": tmpl_row.tax_type,
                                        "rate": tmpl_row.tax_rate,
                                        "description": f"From template {tax_template_name}"
                                    }
                            else:
                                frappe.log_error(
                                    f"Tax account {tmpl_row.tax_type} skipped (belongs to {account_company}, not {company})"
                                )

            if not tax_account_map:
                default_tax_template = frappe.db.get_value(
                    "Sales Taxes and Charges Template",
                    {"company": company, "is_default": 1},
                    "name"
                )
                if default_tax_template:
                    template_doc = frappe.get_doc("Sales Taxes and Charges Template", default_tax_template)
                    for row in template_doc.taxes:
                        tax_account_map[(row.account_head, row.rate)] = {
                            "charge_type": row.charge_type,
                            "account_head": row.account_head,
                            "rate": row.rate,
                            "description": f"From template {default_tax_template}"
                        }
                else:
                    frappe.log_error(
                        f"No default Sales Taxes and Charges Template found for company {company}. Invoice {invoice_name} will likely fail."
                    )

            invoice_data = {
                "doctype": "Sales Invoice",
                "customer": customer,
                "company": company,
                "tax_id": tax_id,
                "posting_date": posting_date,
                "due_date": due_date,
                "currency": currency,
                "conversion_rate": 1,
                "selling_price_list": None,
                "ignore_pricing_rule": 1,
                "debit_to": receivable_account,
                "is_stock_item": 0,
                "items": invoice_items,
                "taxes": list(tax_account_map.values())
            }

            doc = frappe.get_doc(invoice_data)
            doc.insert(ignore_permissions=True, set_name=invoice_name)
            frappe.logger().info(f"✅ Inserted draft invoice: {invoice_name}")

            frappe.db.commit()

        except Exception:
            title = f"Import Error: {inv.get('Naming Series', '')[:20]} - {inv.get('Customer Name', '')[:40]}"
            frappe.log_error(title=title, message=frappe.get_traceback())




##Credit Note

@frappe.whitelist()
def import_credit_notes():
    frappe.set_user("Administrator")
    api_url = "https://adraklive.com/api/l/app.aspx/sales_invoices"
    headers = {"x-api-key": "05884b11-9996-4cc3-b25c-6e3f6cc6139f"}

    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code != 200:
            frappe.log_error("Failed to fetch credit notes", str(response.content))
            return

        try:
            data = response.json()
        except Exception:
            frappe.log_error("Credit Note JSON Decode Error", response.text)
            return

        if data.get("status") != "success":
            frappe.log_error("Invalid response from API", str(data))
            return

        for cn in data.get("payloads", []):
            try:
                credit_note_name = cn.get("Naming Series")
                if not credit_note_name:
                    frappe.log_error("Missing Naming Series in payload", str(cn))
                    continue

                if frappe.db.exists("Sales Invoice", credit_note_name):
                    frappe.logger().info(f"⏩ Skipping duplicate credit note: {credit_note_name}")
                    continue

                customer = cn.get("Customer Name")
                company = cn.get("Company Name")
                tax_id = cn.get("Tax ID")
                posting_date = getdate(cn.get("Posting Date"))
                due_date = today()
                address_title = cn.get("Customer Address")
                items = cn.get("Items", [])

                if not frappe.db.exists("Company", company):
                    frappe.log_error(f"Company '{company}' not found. Credit Note skipped.")
                    continue

                company_doc = frappe.get_doc("Company", company)
                currency = company_doc.default_currency or "SAR"
                income_account = company_doc.default_income_account
                receivable_account = company_doc.default_receivable_account

                if not income_account or not receivable_account:
                    frappe.log_error(f"Missing accounts in company: {company}. Credit Note skipped.")
                    continue

                if frappe.db.exists("Customer", customer):
                    customer_doc = frappe.get_doc("Customer", customer)
                    if tax_id and customer_doc.tax_id != tax_id:
                        customer_doc.tax_id = tax_id
                        customer_doc.save(ignore_permissions=True)
                else:
                    frappe.get_doc({
                        "doctype": "Customer",
                        "customer_name": customer,
                        "customer_type": "Company",
                        "tax_id": tax_id
                    }).insert(ignore_permissions=True)

                if address_title and not address_exists_with_link(address_title, customer):
                    frappe.get_doc({
                        "doctype": "Address",
                        "address_title": address_title,
                        "address_type": "Billing",
                        "address_line1": address_title,
                        "city": "Riyadh",
                        "country": "Saudi Arabia",
                        "links": [{
                            "link_doctype": "Customer",
                            "link_name": customer
                        }]
                    }).insert(ignore_permissions=True)

                credit_note_items = []
                tax_account_map = {}

                for item in items:
                    item_code = item.get("Item_Code")
                    item_name = item.get("Item_Name")
                    description = item.get("Description") or item_name

                    qty = item.get("Qty", 1)
                    if not qty or qty == 0:
                        frappe.logger().warning(f"⚠️ Qty=0 for {item_code}, converting to -1")
                        qty = -1

                    rate = item.get("Rate", 0)
                    uom = item.get("Uom", "Nos")
                    tax_template = item.get("Item_Tax_Template")

                    if uom and not frappe.db.exists("UOM", uom):
                        frappe.get_doc({"doctype": "UOM", "uom_name": uom}).insert(ignore_permissions=True)

                    if not frappe.db.exists("Item", item_code):
                        item_doc = frappe.get_doc({
                            "doctype": "Item",
                            "item_code": item_code,
                            "item_name": item_name,
                            "description": description,
                            "stock_uom": uom,
                            "is_sales_item": 1,
                            "item_group": "Services",
                            "is_stock_item": 0
                        })

                        tax_template_name = frappe.db.get_value(
                            "Item Tax Template",
                            {"name": tax_template, "company": company},
                            "name"
                        )
                        if tax_template_name:
                            item_doc.append("taxes", {"item_tax_template": tax_template_name})

                        item_doc.insert(ignore_permissions=True)

                    credit_note_items.append({
                        "item_code": item_code,
                        "item_name": item_name,
                        "description": description,
                        "qty": qty,
                        "rate": rate,
                        "uom": uom,
                        "income_account": income_account
                    })

                    if tax_template:
                        tax_template_name = frappe.db.get_value(
                            "Item Tax Template",
                            {"name": tax_template, "company": company},
                            "name"
                        )
                        if tax_template_name:
                            template = frappe.get_doc("Item Tax Template", tax_template_name)
                            for tmpl_row in template.taxes:
                                account_company = frappe.db.get_value("Account", tmpl_row.tax_type, "company")
                                if account_company == company:
                                    key = (tmpl_row.tax_type, tmpl_row.tax_rate)
                                    if key not in tax_account_map:
                                        tax_account_map[key] = {
                                            "charge_type": "On Net Total",
                                            "account_head": tmpl_row.tax_type,
                                            "rate": tmpl_row.tax_rate,
                                            "description": f"From template {tax_template_name}"
                                        }
                                else:
                                    frappe.logger().warning(
                                        f"⚠️ Skipping tax account {tmpl_row.tax_type} (belongs to {account_company}, not {company})"
                                    )

                if not tax_account_map:
                    default_tax_template = frappe.db.get_value(
                        "Sales Taxes and Charges Template",
                        {"company": company, "is_default": 1},
                        "name"
                    )
                    if default_tax_template:
                        template_doc = frappe.get_doc("Sales Taxes and Charges Template", default_tax_template)
                        for row in template_doc.taxes:
                            tax_account_map[(row.account_head, row.rate)] = {
                                "charge_type": row.charge_type,
                                "account_head": row.account_head,
                                "rate": row.rate,
                                "description": f"From template {default_tax_template}"
                            }
                    else:
                        frappe.log_error(
                            f"No default Sales Taxes and Charges Template found for company {company}. Credit Note {credit_note_name} will likely fail."
                        )

                cn_data = {
                    "doctype": "Sales Invoice",  
                    "is_return": 1,
                    "return_against": cn.get("Against Invoice"),
                    "customer": customer,
                    "company": company,
                    "tax_id": tax_id,
                    "posting_date": posting_date,
                    "due_date": due_date,
                    "currency": currency,
                    "conversion_rate": 1,
                    "custom_return_reason": cn.get("CUSTOMER_RETURN_REASON"),
                    "ignore_pricing_rule": 1,
                    "debit_to": receivable_account,
                    "items": credit_note_items,
                    "taxes": list(tax_account_map.values())
                }

                doc = frappe.get_doc(cn_data)
                doc.insert(ignore_permissions=True, set_name=credit_note_name)  
                frappe.logger().info(f"✅ Inserted credit note in Draft: {credit_note_name}")

                frappe.db.commit()

            except Exception:
                title = f"Import Error: {cn.get('Naming Series', '')[:20]} - {cn.get('Customer Name', '')[:40]}"
                frappe.log_error(title=title, message=frappe.get_traceback())

    except Exception:
        frappe.log_error("Credit Note Import Fatal Error", frappe.get_traceback())


def address_exists_with_link(address_title, customer):
    address_name = frappe.db.exists("Address", {"address_title": address_title})
    if not address_name:
        return False

    links = frappe.db.get_all(
        "Dynamic Link",
        filters={
            "parenttype": "Address",
            "parent": address_name,
            "link_doctype": "Customer",
            "link_name": customer
        },
        limit=1
    )
    return bool(links)



@frappe.whitelist(allow_guest=True)
def get_all_submitted_invoices_with_qr():
    try:
        invoices = frappe.get_all(
            "Sales Invoice",
            filters={"docstatus": 1},
            fields=["name", "customer", "posting_date", "grand_total", "total_taxes_and_charges"],
            order_by="posting_date desc"
        )

        result = []

        for inv in invoices:
            try:
                qr_code_base64 = get_zatca_phase_1_qr_for_invoice(inv.name)
                result.append({
                    "name": inv.name,
                    "customer": inv.customer,
                    "posting_date": inv.posting_date,
                    "grand_total": inv.grand_total,
                    "total_taxes_and_charges": inv.total_taxes_and_charges,
                    "qr_code": f"data:image/png;base64,{qr_code_base64}" if qr_code_base64 else None
                })
            except Exception as e:
                frappe.log_error(frappe.get_traceback(), f"QR Generation Failed for {inv.name}")

        return {
            "status": "success",
            "count": len(result),
            "invoices": result
        }

    except Exception:
        frappe.log_error(frappe.get_traceback(), "Error in fetching submitted invoices with QR")
        return {
            "status": "error",
            "message": "Failed to fetch submitted invoices"
        }
