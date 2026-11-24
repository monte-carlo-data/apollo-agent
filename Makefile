.PHONY: default clean install generate-docs publish-docs image-check image-checks test-image-checks
ENVIRONMENT_NAME=venv
DOCS_IN_PATH=./docs
DOCS_OUT_PATH=./docs/build
VERSION=local

default:
	@echo "Read the readme"

clean:
	rm -rf $(ENVIRONMENT_NAME) $(DOCS_OUT_PATH)

install: clean
	virtualenv $(ENVIRONMENT_NAME); \
	. $(ENVIRONMENT_NAME)/bin/activate; \
	pip install -r requirements.txt -r requirements-dev.txt;

generate-docs: install
	. $(ENVIRONMENT_NAME)/bin/activate; \
	mkdir -p $(DOCS_OUT_PATH)/js; \
	mkdir -p $(DOCS_OUT_PATH)/css; \
	mkdir -p $(DOCS_OUT_PATH)/img; \
	cp $(DOCS_IN_PATH)/index.html $(DOCS_OUT_PATH); \
	cp $(DOCS_IN_PATH)/js/* $(DOCS_OUT_PATH)/js; \
	cp $(DOCS_IN_PATH)/css/* $(DOCS_OUT_PATH)/css; \
	cp $(DOCS_IN_PATH)/img/* $(DOCS_OUT_PATH)/img; \
	sed -r "s/_version_/${VERSION}/" $(DOCS_IN_PATH)/swagger_template.json > $(DOCS_IN_PATH)/swagger_template_updated.json; \
	flaskswagger apollo.interfaces.generic.main:app --out-dir $(DOCS_OUT_PATH) --template $(DOCS_IN_PATH)/swagger_template_updated.json; \
	curl https://unpkg.com/swagger-ui-dist@4.5.0/swagger-ui-bundle.js > $(DOCS_OUT_PATH)/js/swagger-ui-bundle.js; \
	curl https://unpkg.com/swagger-ui-dist@4.5.0/swagger-ui.css > $(DOCS_OUT_PATH)/css/swagger-ui.css

publish-docs: generate-docs
	aws s3 cp $(DOCS_OUT_PATH)/ s3://${APOLLO_DOCS_BUCKET} --recursive
	AWS_MAX_ATTEMPTS=10 aws cloudfront create-invalidation --distribution-id=${APOLLO_DOCS_CLOUDFRONT_DIST} --paths "/*" --no-cli-pager

# Image contract checks (docs/image-contract-tests.md). Builds one shipped image for
# linux/amd64 and runs the same checks CI runs: make image-check TARGET=lambda
IMAGE_TARGETS=generic cloudrun lambda azure
TARGET?=generic
image-check:
	docker buildx build --platform linux/amd64 --target $(TARGET) \
		--build-arg code_version=0.0.0-local --build-arg build_number=0 \
		--tag apollo-check:$(TARGET) --load .
	python3 scripts/image_checks/check_image.py --target $(TARGET) --image apollo-check:$(TARGET) \
		--expected-version 0.0.0-local,0

image-checks:
	@for t in $(IMAGE_TARGETS); do $(MAKE) image-check TARGET=$$t || exit 1; done

test-image-checks:
	python3 -m pytest -p no:cacheprovider tests/test_image_checks.py tests/test_image_checks_pipeline.py
