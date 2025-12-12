.PHONY: default clean install generate-docs publish-docs
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
