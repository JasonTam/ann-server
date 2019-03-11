
FN_AWS_NAME='ann-serve-test'
FN_FILE_NAME='lambda_function.py'
ZIP_NAME='function.zip'

install-libs:
	mkdir -p package \
	&& cd package \
	&& pip install s3fs --no-deps --target . \
	&& pip install annoy --target .

build:
	cd package \
	&& zip -r9 ../${ZIP_NAME} . \
	&& cd ../ \
	&& zip -g ${ZIP_NAME} ${FN_FILE_NAME}

publish:
	aws lambda \
	--profile lambda-pub \
	update-function-code --function-name ${FN_AWS_NAME} \
	--zip-file fileb://${ZIP_NAME}
	
