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

                    if tax_template and frappe.db.exists("Item Tax Template", tax_template):
                        item_doc.append("taxes", {
                            "item_tax_template": tax_template
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

                if tax_template and frappe.db.exists("Item Tax Template", tax_template):
                    template = frappe.get_doc("Item Tax Template", tax_template)
                    for tmpl_row in template.taxes:
                        key = (tmpl_row.tax_type, tmpl_row.tax_rate)
                        if key not in tax_account_map:
                            tax_account_map[key] = {
                                "charge_type": "On Net Total",
                                "account_head": tmpl_row.tax_type,
                                "rate": tmpl_row.tax_rate,
                                "description": f"From template {tax_template}"
                            }

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
            try:
                doc.submit()
                frappe.logger().info(f"✅ Inserted and submitted invoice: {invoice_name}")
            except Exception:
                frappe.log_error(f"❌ Failed to submit new invoice: {invoice_name}", frappe.get_traceback())

            frappe.db.commit()

        except Exception:
            title = f"Import Error: {inv.get('Naming Series', '')[:20]} - {inv.get('Customer Name', '')[:40]}"
            frappe.log_error(title=title, message=frappe.get_traceback())





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
