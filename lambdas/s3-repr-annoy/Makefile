
FN_AWS_NAME='avro-repr-annoy'
FN_FILE_NAME='lambda_function.py'
ZIP_NAME='function.zip'

pip_args := install \
                --platform manylinux1_x86_64 \
                --only-binary=:all: \
                --target .

install-libs:
	mkdir -p package \
	&& cd package \
	&& git clone https://github.com/dim1tri/snappy-for-aws-lambda.git . \
	&& pip ${pip_args} fastavro \
	&& pip ${pip_args} s3fs --no-deps \
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
	
