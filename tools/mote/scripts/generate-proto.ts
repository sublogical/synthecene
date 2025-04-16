const path = require('path');
const shell = require('shelljs');

const PROTO_DIR = path.join(__dirname, '../../../pg/glimmer/src');
const OUT_DIR = path.join(__dirname, '../proto-generated');

// Ensure output directory exists
shell.mkdir('-p', OUT_DIR);

// Generate TypeScript definitions
const command = `grpc_tools_node_protoc \
    --plugin=protoc-gen-grpc-web=./node_modules/.bin/protoc-gen-grpc-web \
    --js_out=import_style=commonjs,binary:${OUT_DIR} \
    --grpc-web_out=import_style=commonjs+dts,mode=grpcwebtext:${OUT_DIR} \
    -I ${PROTO_DIR} \
    ${PROTO_DIR}/glimmer_api.proto`;

if (shell.exec(command).code !== 0) {
    shell.echo('Error: Proto generation failed');
    shell.exit(1);
} 
