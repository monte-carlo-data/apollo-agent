.PHONY: default clean install generate-docs publish-docs
ENVIRONMENT_NAME=venv
DOCS_IN_PATH=./docs
DOCS_OUT_PATH=./docs/build
VERSION=local
# swagger-ui-dist 4.5.0 as served by unpkg and cdn.jsdelivr.net (identical bytes)
SWAGGER_UI_BUNDLE_SHA256=baddf79c44d4c934dd6a5a465baed050a1c6dd12c1bc801c2e4b1e729e4da4a8
SWAGGER_UI_CSS_SHA256=7f40ecf1323f1faad1f4c6f3c3fd92478cc3135eeccb3a8861d3acad6b61f285

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
	curl -fsSL https://unpkg.com/swagger-ui-dist@4.5.0/swagger-ui-bundle.js > $(DOCS_OUT_PATH)/js/swagger-ui-bundle.js; \
	curl -fsSL https://unpkg.com/swagger-ui-dist@4.5.0/swagger-ui.css > $(DOCS_OUT_PATH)/css/swagger-ui.css; \
	echo "$(SWAGGER_UI_BUNDLE_SHA256)  $(DOCS_OUT_PATH)/js/swagger-ui-bundle.js" | (sha256sum -c - || shasum -a 256 -c -); \
	echo "$(SWAGGER_UI_CSS_SHA256)  $(DOCS_OUT_PATH)/css/swagger-ui.css" | (sha256sum -c - || shasum -a 256 -c -)

publish-docs: generate-docs
	aws s3 cp $(DOCS_OUT_PATH)/ s3://${APOLLO_DOCS_BUCKET} --recursive
	AWS_MAX_ATTEMPTS=10 aws cloudfront create-invalidation --distribution-id=${APOLLO_DOCS_CLOUDFRONT_DIST} --paths "/*" --no-cli-pager
