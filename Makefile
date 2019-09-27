hash:=$(shell git rev-parse --short HEAD)

.PHONY: HKafkaQueue all clean check

HKafkaQueue :
	mkdir -p build/
	go build -o build/HKafkaQueue 

all:
	make check
	@echo $(hash)
	mkdir -p build/

	GOOS=windows GOARCH=amd64 go build -o build/hkafkaqueue-windows-x64-$(hash).exe
	GOOS=windows GOARCH=386 go build -o build/hkafkaqueue-windows-386-$(hash).exe
	GOOS=linux GOARCH=amd64 go build -o build/hkafkaqueue-linux-x64-$(hash)
	GOOS=linux GOARCH=386 go build -o build/hkafkaqueue-linux-386-$(hash)
	GOOS=darwin GOARCH=amd64 go build -o build/hkafkaqueue-darwin-x64-$(hash)

clean:
	rm -rf build/*

check:
	git diff-index --quiet HEAD --
	dep check
