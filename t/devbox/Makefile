
SHELL=bash
.PHONY: test lmd_build provision

INTERACTIVE:=$(shell [ -t 0 ] && echo 1)
ifdef INTERACTIVE
  DOCKER_COMPOSE_TTY=
  DOCKER_COMPOSE_LOGS=
else
  DOCKER_COMPOSE_TTY=-T
  DOCKER_COMPOSE_LOGS=--no-color
endif

test: lmd_build box_prepared
	timeout --foreground -s KILL -v 10m docker compose exec $(DOCKER_COMPOSE_TTY) --user root devbox sudo -iu demo /box/tests/local_test.sh 0

test_verbose: lmd_build box_prepared
	timeout --foreground -s KILL -v 10m docker compose exec $(DOCKER_COMPOSE_TTY) --user root devbox sudo -iu demo /box/tests/local_test.sh 1

prepare:
	timeout --foreground -s KILL -v 30m docker compose build
	docker compose up --remove-orphans -d
	docker ps
	@docker compose logs -t $(DOCKER_COMPOSE_LOGS) -f | while read LOGLINE; do \
		echo "$${LOGLINE}"; \
		[[ "$${LOGLINE}" == *"starting Apache web server"* ]] && pkill -P $$$$ "docker" -9 && exit 0; \
		[[ "$${LOGLINE}" == *"ERROR"* ]]                      && pkill -P $$$$ "docker" -9 && exit 1; \
		[[ "$${LOGLINE}" == *"exited with code"* ]]           && pkill -P $$$$ "docker" -9 && exit 1; \
	done
	$(MAKE) provision

provision:
	timeout --foreground -s KILL -v 30m docker exec $(DOCKER_COMPOSE_TTY) "devbox-devbox-1" env TERM=xterm bash -c "/box/provision/ansible.sh"

box_prepared:
	@[ $$(docker compose ps | grep devbox | grep Up | wc -l) -eq 1 ] || $(MAKE) prepare

update:
	docker compose pull
	for IMG in $$(grep FROM Dockerfile | awk '{ print $$2 }' | sort -u); do docker pull $$IMG; done

lmd_build:
	@test -f ../../lmd || $(MAKE) -C ../.. build

shell:
	docker exec -ti "devbox-devbox-1" env TERM=xterm bash -l

clean:
	docker compose kill
	docker compose rm -f
	-docker network prune -f
