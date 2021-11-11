import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv

import requests

load_dotenv()

whales = [] #define whale variable for use between functions


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
    ovens = requests.get(
            'https://kolibri-data.s3.amazonaws.com/mainnet/oven-data.json'
        )


    for oven in ovens.json()['allOvenData']:
        if int(float(oven['outstandingTokens'])) > whaleSize:
            whales.append(oven['ovenOwner'])



def fetch_plenty_activity(since_hash=None):
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
            'https://api.better-call.dev/v1/tokens/mainnet/transfers/KT1UNBvCJXiwJY6tmHM7CJUVwNPew53XkSfh',
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
  
        
def fetch_quipuswap_activity(since_hash=None):
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
    

latest_quipu_ophash = None
latest_plenty_ophash = None

def fetch_all_new_transfers(quipuhash,plentyhash):
    all_transfers = []
    
    new_quipu_transfers = fetch_quipuswap_activity(since_hash=quipuhash)
    new_plenty_transfers = fetch_plenty_activity(since_hash=plentyhash)

    all_transfers = new_quipu_transfers + new_plenty_transfers

    try:
        latest_quipu_ophash = new_quipu_transfers[0]['hash']
    except IndexError:
        latest_quipu_ophash = quipuhash
    try:
        latest_plenty_ophash = new_plenty_transfers[0]['hash']
    except IndexError:
        latest_plenty_ophash = plentyhash
    
    return all_transfers, latest_quipu_ophash, latest_plenty_ophash

def shorten_address(address):
    return address[:5] + "..." + address[-5:]

def handle_new_transfers(transfers):
    

    #quipuswap trades
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


    #plenty trades
    for tx in transfers:
        payload_plenty = {}
        if tx['parent'] == 'Swap' and tx['from'] != 'KT1UNBvCJXiwJY6tmHM7CJUVwNPew53XkSfh':
            payload_plenty["content"] = "<:plenty:897260943090798622> {} sold :chart_with_downwards_trend: **{:,} kUSD** - **[TX](<{}>)**".format(
                '**[{}](<https://tzkt.io/{}>)**'.format(shorten_address(tx['from']), tx['from']),
                round(int(tx['amount']) / 1e18, 2),
                'https://better-call.dev/mainnet/opg/{}'.format(tx['hash'])
            )

            if tx['from'] in whales:
                payload_plenty["content"] = payload_plenty["content"] + " :whale:"
                

        elif tx['parent'] == 'Swap':
            payload_plenty["content"] = "<:plenty:897260943090798622> {} bought :chart_with_upwards_trend: **{:,} kUSD** - **[TX](<{}>)**".format(
                '**[{}](<https://tzkt.io/{}>)**'.format(shorten_address(tx['to']), tx['to']),
                round(int(tx['amount']) / 1e18, 2),
                'https://better-call.dev/mainnet/opg/{}'.format(tx['hash'])
            )

            if tx['to'] in whales:
                payload_plenty["content"] = payload_plenty["content"] + " :whale:"
        else:
            continue

        send_discord(payload_plenty)

        


def watch_for_changes():

    fetch_whales()
        
    if os.path.exists('.shared/previous-state.json'):
        print("Found .shared/previous-state.json, bootstrapping from that!")
        with open('.shared/previous-state.json') as f:
            previous_state = json.loads(f.read())
            print("Previous state - {}".format(previous_state))
            latest_quipu_ophash = previous_state['latest-quipu-hash']
            latest_plenty_ophash = previous_state['latest-plenty-hash']
    else:
        print("No previous state found! Starting fresh...")
        bcd_payload_quipu = fetch_quipuswap_activity()
        bcd_payload_plenty = fetch_plenty_activity()
        latest_quipu_ophash = bcd_payload_quipu[0]['hash']
        latest_plenty_ophash = bcd_payload_plenty[0]['hash']

    while True:
        try:
            new_transfers, latest_quipu_ophash, latest_plenty_ophash = fetch_all_new_transfers(latest_quipu_ophash,latest_plenty_ophash)
        except Exception as exc:
            print("Exception processing contract activity! Sleeping for a bit and trying again...", exc)
            time.sleep(10)
            continue

        if len(new_transfers) != 0:
            handle_new_transfers(new_transfers[::-1])

            if not os.path.exists('.shared/previous-state.json'):
                if not os.path.exists('.shared'):
                    os.makedirs('.shared')
            with open('.shared/previous-state.json', 'w') as f:
                payload = json.dumps({
                    "latest-quipu-hash": latest_quipu_ophash,
                    "latest-plenty-hash": latest_plenty_ophash,
                    "updated": str(datetime.now())
                })
                f.write(payload)
        else:
            print("[{}] No new activity, looping...".format(datetime.now()))
            time.sleep(30)

if __name__ == "__main__":
    watch_for_changes()
