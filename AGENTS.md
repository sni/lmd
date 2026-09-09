# LMD

LMD fetches Livestatus data from one or multiple sources and provides a livestatus API.

## Basic Information

- **Language:** Written in Go.

## Rules

- Use `make fmt` instead of `go fmt`.
- Use `make test` instead of `go test`.
- Use `make racetest` instead of `go test -race`.
- Use `make golangci` after making code changes and fix the linter errors.
- Do not create additional Markdown files unless explicitly requested.
- Keep comments short. Do not comment the obvious. Only comment exceptional non-obvious things.
- Avoid duplicate code.
- Do not add unnecessary tests.
- Follow clean code principle and avoid bad practice like ex.: `goto`.
- The final acceptance test is `make citest`.

