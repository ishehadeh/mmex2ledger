# mmex2ledger

[MoneyManagerEx](https://moneymanagerex.org) to (h)ledger journal converter.

## Usage

```sh
$ python3 mmex2ledger.py [MMEX DATABASE]
```

The journal file is written to stdout.

## Limitations

This script isn't comprehensive, and was mostly just made to work well enough for me.
If you're looking to run it yourself you may need to update it.

Current Issues:
- MMEX is single-entry while ledger is double-entry. `mmex2ledger` resolves this by using MMEX categories as accounts. All categories are marked as expense accounts unless it includes "income" or "revenue" in the category name.
- The way MMEX handles stocks is a little odd. Each stock has its own account. Funds are moved to this account, then withdrawn/deposited to buy and sell shares. `mmex2ledger` maintains this model, with the addition of a "commission" subaccount for each stock account. Moving funds to the individual stock accounts is not necessary with ledger, so you may want to remove the extra transactions manually.
- This script doesn't handle liabilities, custom fields, or assets.
- Transactions that don't balance are automatically balanced by adding the difference to `expenses:fees`.
