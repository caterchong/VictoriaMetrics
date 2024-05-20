# All these commands must run from repository root.

vmfile:
	APP_NAME=vmfile $(MAKE) app-local

vmfile-race:
	APP_NAME=vmfile RACE=-race $(MAKE) app-local

vmfile-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker

vmfile-pure-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker-pure

vmfile-linux-amd64-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker-linux-amd64

vmfile-linux-arm-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker-linux-arm

vmfile-linux-arm64-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker-linux-arm64

vmfile-linux-ppc64le-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker-linux-ppc64le

vmfile-linux-386-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker-linux-386

vmfile-darwin-amd64-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker-darwin-amd64

vmfile-darwin-arm64-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker-darwin-arm64

vmfile-freebsd-amd64-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker-freebsd-amd64

vmfile-openbsd-amd64-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker-openbsd-amd64

vmfile-windows-amd64-prod:
	APP_NAME=vmfile $(MAKE) app-via-docker-windows-amd64

package-vmfile:
	APP_NAME=vmfile $(MAKE) package-via-docker

package-vmfile-pure:
	APP_NAME=vmfile $(MAKE) package-via-docker-pure

package-vmfile-amd64:
	APP_NAME=vmfile $(MAKE) package-via-docker-amd64

package-vmfile-arm:
	APP_NAME=vmfile $(MAKE) package-via-docker-arm

package-vmfile-arm64:
	APP_NAME=vmfile $(MAKE) package-via-docker-arm64

package-vmfile-ppc64le:
	APP_NAME=vmfile $(MAKE) package-via-docker-ppc64le

package-vmfile-386:
	APP_NAME=vmfile $(MAKE) package-via-docker-386

publish-vmfile:
	APP_NAME=vmfile $(MAKE) publish-via-docker

vmfile-linux-amd64:
	APP_NAME=vmfile CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmfile-linux-arm:
	APP_NAME=vmfile CGO_ENABLED=0 GOOS=linux GOARCH=arm $(MAKE) app-local-goos-goarch

vmfile-linux-arm64:
	APP_NAME=vmfile CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(MAKE) app-local-goos-goarch

vmfile-linux-ppc64le:
	APP_NAME=vmfile CGO_ENABLED=0 GOOS=linux GOARCH=ppc64le $(MAKE) app-local-goos-goarch

vmfile-linux-s390x:
	APP_NAME=vmfile CGO_ENABLED=0 GOOS=linux GOARCH=s390x $(MAKE) app-local-goos-goarch

vmfile-linux-386:
	APP_NAME=vmfile CGO_ENABLED=0 GOOS=linux GOARCH=386 $(MAKE) app-local-goos-goarch

vmfile-darwin-amd64:
	APP_NAME=vmfile CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmfile-darwin-arm64:
	APP_NAME=vmfile CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(MAKE) app-local-goos-goarch

vmfile-freebsd-amd64:
	APP_NAME=vmfile CGO_ENABLED=0 GOOS=freebsd GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmfile-openbsd-amd64:
	APP_NAME=vmfile CGO_ENABLED=0 GOOS=openbsd GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmfile-windows-amd64:
	GOARCH=amd64 APP_NAME=vmfile $(MAKE) app-local-windows-goarch

vmfile-pure:
	APP_NAME=vmfile $(MAKE) app-local-pure
