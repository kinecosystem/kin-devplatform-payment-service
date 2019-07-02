all:
	trap 'kill %1; kill %2' SIGINT; make run & make worker & make watcher

run:
	. ./local.sh && pipenv run python app.py

shell:
	. ./local.sh && . ./secrets/.secrets && pipenv run ipython


.PHONY: shell run
