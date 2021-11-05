build-docker:
	docker build -t kolibri-quipu-tradebot .

bash:
	docker run --rm -it \
	    -v $$(pwd)/:/shared --workdir /shared \
	    kolibri-quipu-tradebot bash

run:
	docker run --rm -it \
	    -v $$(pwd):/shared --workdir /shared \
	    -e DISCORD_WEBHOOK=$(DISCORD_WEBHOOK) \
	    kolibri-quipu-tradebot \
	    python3 /shared/main.py
