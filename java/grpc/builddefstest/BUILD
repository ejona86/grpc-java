# Targets that test of successful operation of java_grpc_library.bzl

package(
    default_testonly = 1,
)

load("//:java_grpc_library.bzl", "java_grpc_library")

proto_library(
    name = "message_proto",
    srcs = ["message.proto"],
)

proto_library(
    name = "service_proto",
    srcs = ["service.proto"],
    deps = [":message_proto"],
)

java_proto_library(
    name = "service_java_proto",
    deps = [":service_proto"],
)

java_grpc_library(
    name = "service_java_grpc",
    srcs = [":service_proto"],
    deps = [":service_java_proto"],
)

java_lite_proto_library(
    name = "service_java_proto_lite",
    deps = [":service_proto"],
)

java_grpc_library(
    name = "service_java_grpc_lite",
    srcs = [":service_proto"],
    flavor = "lite",
    deps = [":service_java_proto_lite"],
)

proto_library(
    name = "message_and_service_proto",
    srcs = ["message_and_service.proto"],
)

java_proto_library(
    name = "message_and_service_java_proto",
    deps = [":message_and_service_proto"],
)

java_grpc_library(
    name = "message_and_service_java_grpc",
    srcs = [":message_and_service_proto"],
    deps = [":message_and_service_java_proto"],
)

java_lite_proto_library(
    name = "message_and_service_java_proto_lite",
    deps = [":message_and_service_proto"],
)

java_grpc_library(
    name = "message_and_service_java_grpc_lite",
    srcs = [":message_and_service_proto"],
    flavor = "lite",
    deps = [":message_and_service_java_proto_lite"],
)

java_library(
    name = "consumer",
    srcs = ["Consumer.java"],
    deps = [
        ":message_and_service_java_grpc",
        ":service_java_grpc",
    ],
)

java_binary(
    name = "consumer_bin",
    main_class = "DoesNotExist",
    runtime_deps = [":consumer"],
)

android_library(
    name = "consumer_android",
    srcs = ["Consumer.java"],
    deps = [
        ":message_and_service_java_grpc_lite",
        ":service_java_grpc_lite",
        "//api",  # fixes warning for io.grpc.MethodDescriptor not found
    ],
)

android_binary(
    name = "consumer_app",
    aapt_version = "aapt",
    manifest = "AndroidManifest.xml",
    deps = [":consumer_android"],
)
