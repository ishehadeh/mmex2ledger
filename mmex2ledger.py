import json, sys, re, sqlite3, math, copy

# ---------------------------------------
# Data Loading
#   functions for loading data from an `.mmb` file into ID -> Record dictionaries

def mmex_get_categories(db):
    categories = {}
    cursor = db.cursor()
    cursor.execute("SELECT CATEGID, CATEGNAME FROM CATEGORY_V1")
    for (id, category) in cursor.fetchall():
        categories[id] = { "name": category, "subcategories": {} }
    
    cursor.execute("SELECT SUBCATEGID, CATEGID, SUBCATEGNAME FROM SUBCATEGORY_V1")
    for (id, parent_id, subcategory) in cursor.fetchall():
        norm_subcategory = normalize_account_name(subcategory)
        categories[parent_id]["subcategories"][id] = { 'name': subcategory }
    return categories

def mmex_get_currencies(db):
    currencies = {}
    stmt = "SELECT CURRENCYID, CURRENCYNAME, SCALE, CURRENCY_SYMBOL, PFX_SYMBOL, SFX_SYMBOL FROM CURRENCYFORMATS_V1"
    for (id, name, scale, symbol, prefix, suffix) in db.execute(stmt):
        currencies[id] = {
            'name': name,
            'scale': scale,
            'symbol': symbol,
            'prefix': prefix,
            'suffix': suffix
        }
    return currencies


def mmex_get_accounts(db):
    accounts = {}

    # TODO handle loan-related fields
    stmt = "SELECT ACCOUNTID, ACCOUNTNAME, ACCOUNTTYPE, STATUS, NOTES, INITIALBAL, CURRENCYID FROM ACCOUNTLIST_V1"
    for (id, name, typ, status, notes, initial_balance, currency_id) in db.execute(stmt):
        accounts[id] = {
            'name': name,
            'type': typ,
            'status': status,
            'notes': notes,
            'initial_balance': initial_balance,
            'currency_id': currency_id
        }
    return accounts


def mmex_get_payees(db):
    payees = {}
    cursor = db.cursor()
    cursor.execute("SELECT PAYEEID, PAYEENAME FROM PAYEE_V1")
    for (id, name) in cursor.fetchall():
        payees[id] = { 'name': name }
    return payees

def mmex_get_transactions(db):
    transactions = {}
    stmt = """SELECT TRANSID,
                      tx.ACCOUNTID,
                      TOACCOUNTID,
                      PAYEENAME,
                      TRANSCODE,
                      TRANSAMOUNT,
                      TOTRANSAMOUNT,
                      tx.STATUS,
                      TRANSACTIONNUMBER,
                      tx.NOTES,
                      CATEGNAME,
                      SUBCATEGNAME,
                      TRANSDATE,
                      stock.SYMBOL,
                      share.SHARENUMBER,
                      share.SHAREPRICE,
                      share.SHARECOMMISSION
                FROM CHECKINGACCOUNT_V1 as tx
                LEFT JOIN PAYEE_V1 as p ON p.PAYEEID = tx.PAYEEID
                LEFT JOIN CATEGORY_V1 as c ON c.CATEGID = tx.CATEGID
                LEFT JOIN SUBCATEGORY_V1 as sc ON sc.CATEGID = tx.CATEGID AND sc.SUBCATEGID = tx.SUBCATEGID
                LEFT JOIN SHAREINFO_V1 as share ON  share.CHECKINGACCOUNTID = tx.TRANSID
                LEFT JOIN TRANSLINK_V1 as link ON  link.CHECKINGACCOUNTID = tx.TRANSID
                LEFT JOIN STOCK_V1 as stock ON link.LINKTYPE == \"Stock\" AND stock.STOCKID = link.LINKRECORDID"""
    for (id, account_id, to_account_id, payee, typ, amount, to_amount, status, number, notes, category, subcategory, date, stock_symbol, share_count, share_price, share_commission) in db.execute(stmt):
        splits = db.execute("""
            SELECT CATEGNAME, SUBCATEGNAME, SPLITTRANSAMOUNT 
            FROM SPLITTRANSACTIONS_V1 as tx
            LEFT JOIN CATEGORY_V1 as c ON c.CATEGID = tx.CATEGID
            LEFT JOIN SUBCATEGORY_V1 as sc ON sc.CATEGID = tx.CATEGID AND sc.SUBCATEGID = tx.SUBCATEGID
            WHERE TRANSID = ?""", (id,))

        transactions[id] = {
            "account_id": account_id,
            "to_account_id": to_account_id,
            "payee": payee,
            "type": typ,
            "amount": amount,
            "to_amount": to_amount,
            "status": status,
            "number": number,
            "notes": notes,
            "category": category,
            "subcategory": subcategory,
            "shares": None if not stock_symbol else {
                "symbol": stock_symbol,
                "count": share_count,
                "price": share_price,
                "commission": share_commission
            },
            "splits": [
                { "amount": amount,
                  "category": category,
                  "subcategory": subcategory }
                for (category, subcategory, amount) in splits ],
            "date": date
        }
    return transactions

# ---------------------------------------
# Transformations
#     functions to convert MMEX data into hledger data

def reformat_account_name(name):
    # use lowercase names, and '-' as a word separator hledger
    # this is just preference, the only requirement is they don't have 2+ spaces in a row   
    return re.sub("\s", "-", name.lower())

def convert_account_heirarchy(name):
    # I used / to make a fake account heirarchy in mmex
    return name.replace("/", ":")

def quote_commodity(name):
    # hledger only requires commodities containing non-letters to be quoted
    return name if name.isalpha() else '"' + name.replace('"', '\\"') + '"'

def mmex_category_to_ledger_account(category, subcategory = None, add_toplevel = True, income="income", expense="expenses", rename = { "revenue": "income", "expense": "expenses" }):
    account_name = reformat_account_name(f"{category}:{subcategory}" if subcategory else category)
    for original, new in rename.items():
        account_name = re.sub(f"(?:^|\\:){original}(?:\\:|$)", new, account_name)
    if add_toplevel:
        if "income" in account_name:
            if not account_name.startswith(f"{income}:"):
                account_name = f"{income}:{account_name}"
        elif account_name != "expenses":
            account_name = f"{expense}:{account_name}"
    return account_name

def make_missing_mmex_account(id):
    # for some reason, I have some transactions refering to accounts that don't exist
    # this function creates a fake account ledger can use
    return {
            'name': f"mmex-missing_{id}",
            'type': "Cash",
            'status': "Open",
            'notes': "This account was used in transactions, but missing from the account list.",
            'initial_balance': 0.0,
            'currency_id': None
        }

def mmex_transaction_to_ledger_postings(mmex_tx, accounts, currencies):
    # note the returned postings may not balance
    assert(mmex_tx["type"] in ["Deposit", "Withdrawal", "Transfer"])

    primary_account = accounts.get(mmex_tx["account_id"]) or make_missing_mmex_account(mmex_tx["to_account_id"])

    if mmex_tx["type"] == "Transfer":
        target_account = accounts.get(mmex_tx["to_account_id"]) or make_missing_mmex_account(mmex_tx["to_account_id"])
        return [
                {
                    "amount": -mmex_tx["amount"],
                    "commodity": currencies[primary_account["currency_id"]],
                    "account": f"assets:{reformat_account_name(convert_account_heirarchy(primary_account['name']))}" # non-asset accounts aren't handled yet
                },
                { 
                    "amount": mmex_tx["to_amount"],
                    "commodity": currencies[target_account["currency_id"]],
                    "account": f"assets:{reformat_account_name(convert_account_heirarchy(target_account['name']))}"
                }
            ]

    direction = 1 if mmex_tx["type"] == "Deposit" else -1
    commodity = currencies[primary_account["currency_id"]]
    primary_account_name_base = reformat_account_name(convert_account_heirarchy(primary_account['name']))
    postings = [
        {
            "amount": mmex_tx["amount"] * direction,
            "commodity": commodity,
            "account": f"assets:{primary_account_name_base}"
        }
    ]
    if len(mmex_tx["splits"]) > 0:
        for split in mmex_tx["splits"]:
            postings.append({
                "amount": split["amount"] * -direction,
                "commodity": commodity,
                "account": mmex_category_to_ledger_account(split["category"], split["subcategory"])
            })
    else:
        if mmex_tx["shares"]:
            high_prec_commodity = commodity
            if high_prec_commodity["scale"] < 100000000:
                high_prec_commodity = copy.copy(commodity)
                high_prec_commodity["scale"] = 100000000
                

            postings.append({
                "amount": mmex_tx["shares"]["count"],
                "commodity": {
                    "scale": 10000,
                    "symbol": mmex_tx["shares"]["symbol"],
                },
                "price": {
                    "amount": mmex_tx["shares"]["price"],
                    "commodity": high_prec_commodity,
                },
                "account": f"assets:{primary_account_name_base}"
            })
            # if mmex_tx["shares"]["commission"] == 0:
            # force transaction to balance with commission
            mmex_tx["shares"]["commission"] = mmex_tx["amount"] * -direction - mmex_tx["shares"]["price"] * mmex_tx["shares"]["count"]
            if mmex_tx["shares"]["commission"] != 0:
                # print(mmex_tx["shares"]["commission"])
                postings.append({
                    "amount": mmex_tx["shares"]["commission"],
                    "commodity": commodity,
                    "account": f"expenses:{primary_account_name_base}:commission"
                })            
        else:
            postings.append({
                "amount": mmex_tx["amount"] * -direction,
                "commodity": commodity,
                "account": mmex_category_to_ledger_account(mmex_tx["category"], mmex_tx["subcategory"])
            })
            
    return postings

def reformat_notes(notes):
    return notes.replace("\n", "  ")

def mmex_transaction_to_ledger_transaction(mmex_tx, accounts, currencies, force_balance_account="expenses:fees"):
    tags = []

    status_part = " *"
    payee_part = " " + mmex_tx["payee"] if mmex_tx["payee"] else ""
    tags_part = " ;" + " ".join(tags) if tags is not None and len(tags) > 0 else ""
    note = reformat_notes(mmex_tx['notes'])
    if note != "":
        note = " | " + note

    postings = mmex_transaction_to_ledger_postings(mmex_tx, accounts, currencies)
    common_commodity = postings[0]["commodity"] if all(p["commodity"] == postings[0]["commodity"] for p in postings) else None
    balance = sum(p["amount"] for p in postings) if common_commodity else 0
    if balance != 0:
        postings.append({ "account": force_balance_account, "amount": -balance, "commodity": common_commodity })

    hledger_transaction = f"{mmex_tx['date']}{status_part}{payee_part}{note}{tags_part}\n"
    for posting in postings:
        prec = int(math.log10(posting['commodity']['scale']))
        hledger_transaction += f"  {posting['account']}  {posting['amount']:.{prec}f} {quote_commodity(posting['commodity']['symbol'])}"
        if posting.get("price"):
            prec = int(math.log10(posting['price']['commodity']['scale']))
            hledger_transaction += f" @ {posting['price']['amount']:.{prec}f} {posting['price']['commodity']['symbol']}"
        hledger_transaction += "\n"

    return hledger_transaction

if __name__ == "__main__":
    mmex_path = sys.argv[1]
    mmex = sqlite3.connect(mmex_path)
    
    accounts = mmex_get_accounts(mmex)
    currencies = mmex_get_currencies(mmex)
    transactions = mmex_get_transactions(mmex)
    for tx in sorted(transactions.values(), key=lambda x: x.get("date")):
        print(mmex_transaction_to_ledger_transaction(tx, accounts, currencies))
