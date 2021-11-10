import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv

import requests

load_dotenv()

# Configure Discord
try:
    DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]
    if len(DISCORD_WEBHOOK) < 5:
        raise KeyError
except KeyError as e:
    raise Exception("DISCORD_WEBHOOK envar not found! You must set a DISCORD_WEBHOOK for things to work properly.")

def send_discord(payload):
    print("Submitting webhook...")
    response = requests.post(DISCORD_WEBHOOK, json=payload)
    response.raise_for_status()
    if int(response.headers['x-ratelimit-remaining']) == 0:
        rate_limit_reset = float(response.headers['x-ratelimit-reset-after']) + 1
        print("Waiting for discord rate limits...({} sec)".format(rate_limit_reset))
        time.sleep(rate_limit_reset)

    time.sleep(1)

    return response


#fetch whales to mark their trades
def fetch_whales():

    whaleSize = 100000 #size threshold
    whaleSize = whaleSize * 1e18 #convert using mantissa
    global whales
    whales = []
    ovens = requests.get(
            'https://kolibri-data.s3.amazonaws.com/mainnet/oven-data.json'
        )


    for oven in ovens.json()['allOvenData']:
        if int(float(oven['outstandingTokens'])) > whaleSize:
            whales.append(oven['ovenOwner'])


    
def fetch_contract_activity(since_hash=None):
    params = {
        'token_id': 0,
        'size': 10,
        'contracts': 'KT1K9gCRgaLRFKTErYt1wVxA3Frb9FjasjTV',
        'status': 'applied',
    }

    transfers = []

    last_id = None

    while True:
        if last_id is not None:
            params['last_id'] = last_id

        response = requests.get(
            'https://api.better-call.dev/v1/tokens/mainnet/transfers/KT1K4EwTpbvYN9agJdjpyJm4ZZdhpUNKB3F6',
            params=params
        )

        applied_ops = response.json()

        last_id = applied_ops['last_id']

        for operation in applied_ops['transfers']:
            if operation['hash'] == since_hash:
                return transfers
            transfers.append(operation)

        if since_hash is None:
            return transfers

def shorten_address(address):
    return address[:5] + "..." + address[-5:]

def handle_new_transfers(transfers):
    for tx in transfers:
        payload = {}
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

        send_discord(payload)

def watch_for_changes():

    fetch_whales()
    
    
    if os.path.exists('.shared/previous-state.json'):
        print("Found .shared/previous-state.json, bootstrapping from that!")
        with open('.shared/previous-state.json') as f:
            previous_state = json.loads(f.read())
            print("Previous state - {}".format(previous_state))
            latest_trade_hash = previous_state['latest-hash']
    else:
        print("No previous state found! Starting fresh...")
        bcd_payload = fetch_contract_activity()
        latest_trade_hash = bcd_payload[0]['hash']

    while True:
        try:
            new_transfers = fetch_contract_activity(since_hash=latest_trade_hash)
        except Exception as exc:
            print("Exception processing contract activity! Sleeping for a bit and trying again...", exc)
            time.sleep(10)
            continue

        if len(new_transfers) != 0:
            handle_new_transfers(new_transfers[::-1])
            latest_trade_hash = new_transfers[0]['hash']

            if not os.path.exists('.shared/previous-state.json'):
                if not os.path.exists('.shared'):
                    os.makedirs('.shared')
            with open('.shared/previous-state.json', 'w') as f:
                payload = json.dumps({
                    "latest-hash": latest_trade_hash,
                    "updated": str(datetime.now())
                })
                f.write(payload)
        else:
            print("[{}] No new activity, looping...".format(datetime.now()))
            time.sleep(30)

if __name__ == "__main__":
    watch_for_changes()
