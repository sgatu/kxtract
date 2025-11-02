BUILD_VER ?= dev
build-release:
	@rm -rf ./dist
	@mkdir ./dist
	go build -ldflags "-X 'github.com/sgatu/kxtract/internal/config.Version=$(BUILD_VER)'" -o ./dist/kxtract ./cmd/cli/main.go
	@chmod +x ./dist/kxtract
	@cp ./test_cfg.ignore dist/cfg.json
