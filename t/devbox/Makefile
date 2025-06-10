
SHELL=bash
.PHONY: test lmd_build provision

test: lmd_build box_prepared
	docker compose exec --user root devbox sudo -iu demo /box/tests/local_test.sh 0

test_verbose: lmd_build box_prepared
	docker compose exec --user root devbox sudo -iu demo /box/tests/local_test.sh 1

prepare:
	docker compose build
	docker compose up --remove-orphans -d
	docker ps
	@while read LOGLINE; do \
		echo "$${LOGLINE}"; \
		[[ "$${LOGLINE}" == *"starting Apache web server"* ]] && exit 0; \
		[[ "$${LOGLINE}" == *"ERROR"* ]] && exit 1; \
	done < <(docker compose logs -f)
	$(MAKE) provision

provision:
	docker exec -i "devbox-devbox-1" env TERM=xterm bash -c "/box/provision/ansible.sh"

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
