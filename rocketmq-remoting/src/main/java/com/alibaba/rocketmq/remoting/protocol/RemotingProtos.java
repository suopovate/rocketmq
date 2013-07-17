// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: remoting.proto

package com.alibaba.rocketmq.remoting.protocol;

public final class RemotingProtos {
    private RemotingProtos() {
    }

    /**
     * Protobuf enum {@code remoting.RequestCode}
     * 
     * <pre>
     * RPC请求代码
     * </pre>
     */
    public enum RequestCode {
        /**
         * <code>DEMO_REQUEST = 0;</code>
         */
        DEMO_REQUEST(0, 0), ;

        /**
         * <code>DEMO_REQUEST = 0;</code>
         */
        public static final int DEMO_REQUEST_VALUE = 0;


        public final int getNumber() {
            return value;
        }


        public static RequestCode valueOf(int value) {
            switch (value) {
            case 0:
                return DEMO_REQUEST;
            default:
                return null;
            }
        }

        private final int index;
        private final int value;


        private RequestCode(int index, int value) {
            this.index = index;
            this.value = value;
        }

        // @@protoc_insertion_point(enum_scope:remoting.RequestCode)
    }

    /**
     * Protobuf enum {@code remoting.ResponseCode}
     * 
     * <pre>
     * RPC应答代码
     * </pre>
     */
    public enum ResponseCode {
        /**
         * <code>SUCCESS = 0;</code>
         * 
         * <pre>
         * 成功
         * </pre>
         */
        SUCCESS(0, 0),
        /**
         * <code>SYSTEM_ERROR = 1;</code>
         * 
         * <pre>
         * 发生了未捕获异常
         * </pre>
         */
        SYSTEM_ERROR(1, 1),
        /**
         * <code>SYSTEM_BUSY = 2;</code>
         * 
         * <pre>
         * 由于线程池拥堵，系统繁忙
         * </pre>
         */
        SYSTEM_BUSY(2, 2),
        /**
         * <code>REQUEST_CODE_NOT_SUPPORTED = 3;</code>
         * 
         * <pre>
         * 请求代码不支持
         * </pre>
         */
        REQUEST_CODE_NOT_SUPPORTED(3, 3), ;

        /**
         * <code>SUCCESS = 0;</code>
         * 
         * <pre>
         * 成功
         * </pre>
         */
        public static final int SUCCESS_VALUE = 0;
        /**
         * <code>SYSTEM_ERROR = 1;</code>
         * 
         * <pre>
         * 发生了未捕获异常
         * </pre>
         */
        public static final int SYSTEM_ERROR_VALUE = 1;
        /**
         * <code>SYSTEM_BUSY = 2;</code>
         * 
         * <pre>
         * 由于线程池拥堵，系统繁忙
         * </pre>
         */
        public static final int SYSTEM_BUSY_VALUE = 2;
        /**
         * <code>REQUEST_CODE_NOT_SUPPORTED = 3;</code>
         * 
         * <pre>
         * 请求代码不支持
         * </pre>
         */
        public static final int REQUEST_CODE_NOT_SUPPORTED_VALUE = 3;


        public final int getNumber() {
            return value;
        }


        public static ResponseCode valueOf(int value) {
            switch (value) {
            case 0:
                return SUCCESS;
            case 1:
                return SYSTEM_ERROR;
            case 2:
                return SYSTEM_BUSY;
            case 3:
                return REQUEST_CODE_NOT_SUPPORTED;
            default:
                return null;
            }
        }

        private final int index;
        private final int value;


        private ResponseCode(int index, int value) {
            this.index = index;
            this.value = value;
        }

        // @@protoc_insertion_point(enum_scope:remoting.ResponseCode)
    }
}
