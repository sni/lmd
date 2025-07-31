
SHELL=bash
.PHONY: test lmd_build provision

test: lmd_build box_prepared
	timeout -v 10m docker compose exec -T --user root devbox sudo -iu demo /box/tests/local_test.sh 0

test_verbose: lmd_build box_prepared
	timeout -v 10m docker compose exec -T --user root devbox sudo -iu demo /box/tests/local_test.sh 1

prepare:
	timeout -v 30m docker compose build
	docker compose up --remove-orphans -d
	docker ps
	@docker compose logs -t -f | while read LOGLINE; do \
		echo "$${LOGLINE}"; \
		[[ "$${LOGLINE}" == *"starting Apache web server"* ]] && pkill -PIPE -P $$$$ "docker" && exit 0; \
		[[ "$${LOGLINE}" == *"ERROR"* ]]                      && pkill -PIPE -P $$$$ "docker" && exit 1; \
	done
	$(MAKE) provision

provision:
	timeout -v 30m docker exec "devbox-devbox-1" env TERM=xterm bash -c "/box/provision/ansible.sh"

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
