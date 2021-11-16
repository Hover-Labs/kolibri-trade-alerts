import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv

import requests

load_dotenv()

whales = [] # define whale variable for use between functions

QUIPU_KUSD = 'KT1K4EwTpbvYN9agJdjpyJm4ZZdhpUNKB3F6'
PLENTY_KUSD = 'KT1UNBvCJXiwJY6tmHM7CJUVwNPew53XkSfh'
PLENTY_USDTZ_KUSD = 'KT1TnsQ6JqzyTz5PHMsGj28WwJyBtgc146aJ'

# Configure Discord
try:
    DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]
    if len(DISCORD_WEBHOOK) < 5:
        raise KeyError
except KeyError as e:
    raise Exception("DISCORD_WEBHOOK envar not found! You must set a DISCORD_WEBHOOK for things to work properly.")

DISCORD_WEBHOOK_WHALES = os.environ.get("DISCORD_WEBHOOK_WHALES", None)
# If we have an empty string or something, just revert to None
if DISCORD_WEBHOOK_WHALES is not None and len(DISCORD_WEBHOOK_WHALES) < 5:
    DISCORD_WEBHOOK_WHALES = None

def send_discord(payload,channel):
    print("Submitting webhook...")
    response = requests.post(channel, json=payload)
    response.raise_for_status()
    if int(response.headers['x-ratelimit-remaining']) == 0:
        rate_limit_reset = float(response.headers['x-ratelimit-reset-after']) + 1
        print("Waiting for discord rate limits...({} sec)".format(rate_limit_reset))
        time.sleep(rate_limit_reset)

    time.sleep(1)

    return response

# fetch whales with large ovens to mark their trades
def fetch_whales():
    whaleSize = 100_000 # size threshold in kUSD
    whaleSize = whaleSize * 1e18 # convert using mantissa
    ovens = requests.get(
        'https://kolibri-data.s3.amazonaws.com/mainnet/oven-data.json'
    )

    for oven in ovens.json()['allOvenData']:
        if int(float(oven['outstandingTokens'])) > whaleSize:
            whales.append(oven['ovenOwner'])

def fetch_contract_transfers(contract_address, since_hash=None):
    params = {
        "token_id": 0,
        "size": 10,
        "contracts": "KT1K9gCRgaLRFKTErYt1wVxA3Frb9FjasjTV",
    }

    transfers = []

    last_id = None

    while True:
        if last_id is not None:
            params['last_id'] = last_id

        response = requests.get(
            'https://api.better-call.dev/v1/tokens/mainnet/transfers/{}'.format(contract_address),
            params=params
        )

        applied_ops = response.json()

        last_id = applied_ops['last_id']

        for operation in applied_ops['transfers']:
            if operation['hash'] == since_hash:
                return transfers, applied_ops['transfers'][0]['hash']
            if operation['status'] == 'applied':
                transfers.append(operation)

        if since_hash is None:
            return transfers, transfers[0]['hash']

def shorten_address(address):
    return address[:5] + "..." + address[-5:]

def handle_new_transfers(transfers):
    for tx in transfers:
        payload = {}

        # if it is Quipu XTZ/kUSD contract
        if tx['from'] == QUIPU_KUSD or tx['to'] == QUIPU_KUSD:  # if it is plenty/kusd contract
            if tx['parent'] == 'tokenToTezPayment':
                payload["content"] = "<:quipuswap:906262514197749831> {} sold :chart_with_downwards_trend: **{:,} kUSD** - **[TX](<{}>)**".format(
                    '**[{}](<https://tzkt.io/{}>)**'.format(shorten_address(tx['from']), tx['from']),
                    round(int(tx['amount']) / 1e18, 2),
                    'https://better-call.dev/mainnet/opg/{}'.format(tx['hash'])
                )

                if tx['from'] in whales:
                    payload["content"] = payload["content"] + " :whale:"

            elif tx['parent'] == 'tezToTokenPayment':
                payload["content"] = "<:quipuswap:906262514197749831> {} bought :chart_with_upwards_trend: **{:,} kUSD** - **[TX](<{}>)**".format(
                    '**[{}](<https://tzkt.io/{}>)**'.format(shorten_address(tx['to']), tx['to']),
                    round(int(tx['amount']) / 1e18, 2),
                    'https://better-call.dev/mainnet/opg/{}'.format(tx['hash'])
                )

                if tx['to'] in whales:
                    payload["content"] = payload["content"] + " :whale:"

            else:
                continue

        # if it is PLENTY/kUSD contract
        if tx['from'] == PLENTY_KUSD or tx['to'] == PLENTY_KUSD: # if it is plenty/kusd contract
            if tx['parent'] == 'Swap' and tx['from'] != PLENTY_KUSD:
                payload["content"] = "<:plenty:897260943090798622> {} sold :chart_with_downwards_trend: **{:,} kUSD** for **PLENTY** - **[TX](<{}>)**".format(
                    '**[{}](<https://tzkt.io/{}>)**'.format(shorten_address(tx['from']), tx['from']),
                    round(int(tx['amount']) / 1e18, 2),
                    'https://better-call.dev/mainnet/opg/{}'.format(tx['hash'])
                )

                if tx['from'] in whales:
                    payload["content"] = payload["content"] + " :whale:"

            elif tx['parent'] == 'Swap':
                payload["content"] = "<:plenty:897260943090798622> {} bought :chart_with_upwards_trend: **{:,} kUSD** with **PLENTY** - **[TX](<{}>)**".format(
                    '**[{}](<https://tzkt.io/{}>)**'.format(shorten_address(tx['to']), tx['to']),
                    round(int(tx['amount']) / 1e18, 2),
                    'https://better-call.dev/mainnet/opg/{}'.format(tx['hash'])
                )

                if tx['to'] in whales:
                    payload["content"] = payload["content"] + " :whale:"

            else:
                continue

        # if it is USDTz/kUSD contract
        if tx['from'] == PLENTY_USDTZ_KUSD or tx['to'] == PLENTY_USDTZ_KUSD:
            if tx['parent'] == 'Swap' and tx['from'] != PLENTY_USDTZ_KUSD:
                payload["content"] = "<:plenty:897260943090798622> {} sold :chart_with_downwards_trend: **{:,} kUSD** for **USDtz** - **[TX](<{}>)**".format(
                    '**[{}](<https://tzkt.io/{}>)**'.format(shorten_address(tx['from']), tx['from']),
                    round(int(tx['amount']) / 1e18, 2),
                    'https://better-call.dev/mainnet/opg/{}'.format(tx['hash'])
                )

                if tx['from'] in whales:
                    payload["content"] = payload["content"] + " :whale:"

            elif tx['parent'] == 'Swap':
                payload["content"] = "<:plenty:897260943090798622> {} bought :chart_with_upwards_trend: **{:,} kUSD** with **USDtz** - **[TX](<{}>)**".format(
                    '**[{}](<https://tzkt.io/{}>)**'.format(shorten_address(tx['to']), tx['to']),
                    round(int(tx['amount']) / 1e18, 2),
                    'https://better-call.dev/mainnet/opg/{}'.format(tx['hash'])
                )

                if tx['to'] in whales:
                    payload["content"] = payload["content"] + " :whale:"

            else:
                continue

        send_discord(payload, DISCORD_WEBHOOK)
        if int(tx['amount']) / 1e18 >= 5000 and DISCORD_WEBHOOK_WHALES is not None:
            send_discord(payload, DISCORD_WEBHOOK_WHALES)

def watch_for_changes():
    if os.path.exists('.shared/previous-state.json'):
        print("Found .shared/previous-state.json, bootstrapping from that!")
        with open('.shared/previous-state.json') as f:
            previous_state = json.loads(f.read())
            print("Previous state - {}".format(previous_state))
            latest_quipu_ophash = previous_state['latest-quipu-hash']
            latest_plenty_ophash = previous_state['latest-plenty-hash']
            latest_usdtz_plenty_ophash = previous_state['latest-usdtz-plenty-hash']
    else:
        print("No previous state found! Starting fresh...")
        _, latest_quipu_ophash = fetch_contract_transfers(QUIPU_KUSD)
        _, latest_plenty_ophash = fetch_contract_transfers(PLENTY_KUSD)
        _, latest_usdtz_plenty_ophash = fetch_contract_transfers(PLENTY_USDTZ_KUSD)

    while True:
        try:
            new_quipu_transfers, latest_quipu_ophash = fetch_contract_transfers(QUIPU_KUSD, since_hash=latest_quipu_ophash)

            new_plenty_transfers, latest_plenty_ophash = fetch_contract_transfers(PLENTY_KUSD, since_hash=latest_plenty_ophash)

            new_usdtz_plenty_transfers, latest_usdtz_plenty_ophash = fetch_contract_transfers(PLENTY_USDTZ_KUSD, since_hash=latest_usdtz_plenty_ophash)

            all_transfers = new_quipu_transfers + new_plenty_transfers + new_usdtz_plenty_transfers

        except Exception as exc:
            print("Exception processing contract activity! Sleeping for a bit and trying again...", exc)
            time.sleep(10)
            continue

        if len(all_transfers) != 0:
            handle_new_transfers(all_transfers[::-1])

            if not os.path.exists('.shared/previous-state.json'):
                if not os.path.exists('.shared'):
                    os.makedirs('.shared')
            with open('.shared/previous-state.json', 'w') as f:
                payload = json.dumps({
                    "latest-quipu-hash": latest_quipu_ophash,
                    "latest-plenty-hash": latest_plenty_ophash,
                    "latest-usdtz-plenty-hash": latest_usdtz_plenty_ophash,
                    "updated": str(datetime.now())
                })
                f.write(payload)
        else:
            print("[{}] No new activity, looping...".format(datetime.now()))
            time.sleep(30)

if __name__ == "__main__":
    fetch_whales()
    watch_for_changes()
