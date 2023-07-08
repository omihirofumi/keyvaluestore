DEST_DIR := "./kvs"
SOURCE_DIR := "./kvs"

proto:
	@protoc --proto_path=$(SOURCE_DIR) \
		--go_out=$(DEST_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(DEST_DIR) --go-grpc_opt=paths=source_relative \
		$(SOURCE_DIR)/kvs.proto