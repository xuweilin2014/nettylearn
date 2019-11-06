// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: src/protobuf/message.proto

package com.xu.netty.seventhexample.multimsg.proto;

/**
 * Protobuf type {@code com.xu.netty.seventhexample.multimsg.proto.App}
 */
public  final class App extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:com.xu.netty.seventhexample.multimsg.proto.App)
    AppOrBuilder {
private static final long serialVersionUID = 0L;
  // Use App.newBuilder() to construct.
  private App(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private App() {
    ip_ = "";
    phoneType_ = "";
  }

  @java.lang.Override
  @SuppressWarnings({"unused"})
  protected java.lang.Object newInstance(
      UnusedPrivateParameter unused) {
    return new App();
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private App(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new java.lang.NullPointerException();
    }
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          case 10: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000001;
            ip_ = bs;
            break;
          }
          case 18: {
            com.google.protobuf.ByteString bs = input.readBytes();
            bitField0_ |= 0x00000002;
            phoneType_ = bs;
            break;
          }
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return com.xu.netty.seventhexample.multimsg.proto.MyClientInfo.internal_static_com_xu_netty_seventhexample_multimsg_proto_App_descriptor;
  }

  @java.lang.Override
  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.xu.netty.seventhexample.multimsg.proto.MyClientInfo.internal_static_com_xu_netty_seventhexample_multimsg_proto_App_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.xu.netty.seventhexample.multimsg.proto.App.class, com.xu.netty.seventhexample.multimsg.proto.App.Builder.class);
  }

  private int bitField0_;
  public static final int IP_FIELD_NUMBER = 1;
  private volatile java.lang.Object ip_;
  /**
   * <code>optional string ip = 1;</code>
   * @return Whether the ip field is set.
   */
  public boolean hasIp() {
    return ((bitField0_ & 0x00000001) != 0);
  }
  /**
   * <code>optional string ip = 1;</code>
   * @return The ip.
   */
  public java.lang.String getIp() {
    java.lang.Object ref = ip_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        ip_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string ip = 1;</code>
   * @return The bytes for ip.
   */
  public com.google.protobuf.ByteString
      getIpBytes() {
    java.lang.Object ref = ip_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      ip_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int PHONETYPE_FIELD_NUMBER = 2;
  private volatile java.lang.Object phoneType_;
  /**
   * <code>optional string phoneType = 2;</code>
   * @return Whether the phoneType field is set.
   */
  public boolean hasPhoneType() {
    return ((bitField0_ & 0x00000002) != 0);
  }
  /**
   * <code>optional string phoneType = 2;</code>
   * @return The phoneType.
   */
  public java.lang.String getPhoneType() {
    java.lang.Object ref = phoneType_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (bs.isValidUtf8()) {
        phoneType_ = s;
      }
      return s;
    }
  }
  /**
   * <code>optional string phoneType = 2;</code>
   * @return The bytes for phoneType.
   */
  public com.google.protobuf.ByteString
      getPhoneTypeBytes() {
    java.lang.Object ref = phoneType_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      phoneType_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  private byte memoizedIsInitialized = -1;
  @java.lang.Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @java.lang.Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (((bitField0_ & 0x00000001) != 0)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, ip_);
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, phoneType_);
    }
    unknownFields.writeTo(output);
  }

  @java.lang.Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (((bitField0_ & 0x00000001) != 0)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, ip_);
    }
    if (((bitField0_ & 0x00000002) != 0)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, phoneType_);
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof com.xu.netty.seventhexample.multimsg.proto.App)) {
      return super.equals(obj);
    }
    com.xu.netty.seventhexample.multimsg.proto.App other = (com.xu.netty.seventhexample.multimsg.proto.App) obj;

    if (hasIp() != other.hasIp()) return false;
    if (hasIp()) {
      if (!getIp()
          .equals(other.getIp())) return false;
    }
    if (hasPhoneType() != other.hasPhoneType()) return false;
    if (hasPhoneType()) {
      if (!getPhoneType()
          .equals(other.getPhoneType())) return false;
    }
    if (!unknownFields.equals(other.unknownFields)) return false;
    return true;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasIp()) {
      hash = (37 * hash) + IP_FIELD_NUMBER;
      hash = (53 * hash) + getIp().hashCode();
    }
    if (hasPhoneType()) {
      hash = (37 * hash) + PHONETYPE_FIELD_NUMBER;
      hash = (53 * hash) + getPhoneType().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static com.xu.netty.seventhexample.multimsg.proto.App parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.xu.netty.seventhexample.multimsg.proto.App parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.xu.netty.seventhexample.multimsg.proto.App parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.xu.netty.seventhexample.multimsg.proto.App parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.xu.netty.seventhexample.multimsg.proto.App parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.xu.netty.seventhexample.multimsg.proto.App parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.xu.netty.seventhexample.multimsg.proto.App parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.xu.netty.seventhexample.multimsg.proto.App parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.xu.netty.seventhexample.multimsg.proto.App parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static com.xu.netty.seventhexample.multimsg.proto.App parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.xu.netty.seventhexample.multimsg.proto.App parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.xu.netty.seventhexample.multimsg.proto.App parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @java.lang.Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(com.xu.netty.seventhexample.multimsg.proto.App prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @java.lang.Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code com.xu.netty.seventhexample.multimsg.proto.App}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:com.xu.netty.seventhexample.multimsg.proto.App)
      com.xu.netty.seventhexample.multimsg.proto.AppOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.xu.netty.seventhexample.multimsg.proto.MyClientInfo.internal_static_com_xu_netty_seventhexample_multimsg_proto_App_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.xu.netty.seventhexample.multimsg.proto.MyClientInfo.internal_static_com_xu_netty_seventhexample_multimsg_proto_App_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.xu.netty.seventhexample.multimsg.proto.App.class, com.xu.netty.seventhexample.multimsg.proto.App.Builder.class);
    }

    // Construct using com.xu.netty.seventhexample.multimsg.proto.App.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    @java.lang.Override
    public Builder clear() {
      super.clear();
      ip_ = "";
      bitField0_ = (bitField0_ & ~0x00000001);
      phoneType_ = "";
      bitField0_ = (bitField0_ & ~0x00000002);
      return this;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.xu.netty.seventhexample.multimsg.proto.MyClientInfo.internal_static_com_xu_netty_seventhexample_multimsg_proto_App_descriptor;
    }

    @java.lang.Override
    public com.xu.netty.seventhexample.multimsg.proto.App getDefaultInstanceForType() {
      return com.xu.netty.seventhexample.multimsg.proto.App.getDefaultInstance();
    }

    @java.lang.Override
    public com.xu.netty.seventhexample.multimsg.proto.App build() {
      com.xu.netty.seventhexample.multimsg.proto.App result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @java.lang.Override
    public com.xu.netty.seventhexample.multimsg.proto.App buildPartial() {
      com.xu.netty.seventhexample.multimsg.proto.App result = new com.xu.netty.seventhexample.multimsg.proto.App(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        to_bitField0_ |= 0x00000001;
      }
      result.ip_ = ip_;
      if (((from_bitField0_ & 0x00000002) != 0)) {
        to_bitField0_ |= 0x00000002;
      }
      result.phoneType_ = phoneType_;
      result.bitField0_ = to_bitField0_;
      onBuilt();
      return result;
    }

    @java.lang.Override
    public Builder clone() {
      return super.clone();
    }
    @java.lang.Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.setField(field, value);
    }
    @java.lang.Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return super.clearField(field);
    }
    @java.lang.Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return super.clearOneof(oneof);
    }
    @java.lang.Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return super.setRepeatedField(field, index, value);
    }
    @java.lang.Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return super.addRepeatedField(field, value);
    }
    @java.lang.Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof com.xu.netty.seventhexample.multimsg.proto.App) {
        return mergeFrom((com.xu.netty.seventhexample.multimsg.proto.App)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.xu.netty.seventhexample.multimsg.proto.App other) {
      if (other == com.xu.netty.seventhexample.multimsg.proto.App.getDefaultInstance()) return this;
      if (other.hasIp()) {
        bitField0_ |= 0x00000001;
        ip_ = other.ip_;
        onChanged();
      }
      if (other.hasPhoneType()) {
        bitField0_ |= 0x00000002;
        phoneType_ = other.phoneType_;
        onChanged();
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    @java.lang.Override
    public final boolean isInitialized() {
      return true;
    }

    @java.lang.Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      com.xu.netty.seventhexample.multimsg.proto.App parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (com.xu.netty.seventhexample.multimsg.proto.App) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private java.lang.Object ip_ = "";
    /**
     * <code>optional string ip = 1;</code>
     * @return Whether the ip field is set.
     */
    public boolean hasIp() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional string ip = 1;</code>
     * @return The ip.
     */
    public java.lang.String getIp() {
      java.lang.Object ref = ip_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          ip_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string ip = 1;</code>
     * @return The bytes for ip.
     */
    public com.google.protobuf.ByteString
        getIpBytes() {
      java.lang.Object ref = ip_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        ip_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string ip = 1;</code>
     * @param value The ip to set.
     * @return This builder for chaining.
     */
    public Builder setIp(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      ip_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string ip = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearIp() {
      bitField0_ = (bitField0_ & ~0x00000001);
      ip_ = getDefaultInstance().getIp();
      onChanged();
      return this;
    }
    /**
     * <code>optional string ip = 1;</code>
     * @param value The bytes for ip to set.
     * @return This builder for chaining.
     */
    public Builder setIpBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
      ip_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object phoneType_ = "";
    /**
     * <code>optional string phoneType = 2;</code>
     * @return Whether the phoneType field is set.
     */
    public boolean hasPhoneType() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional string phoneType = 2;</code>
     * @return The phoneType.
     */
    public java.lang.String getPhoneType() {
      java.lang.Object ref = phoneType_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          phoneType_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>optional string phoneType = 2;</code>
     * @return The bytes for phoneType.
     */
    public com.google.protobuf.ByteString
        getPhoneTypeBytes() {
      java.lang.Object ref = phoneType_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        phoneType_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>optional string phoneType = 2;</code>
     * @param value The phoneType to set.
     * @return This builder for chaining.
     */
    public Builder setPhoneType(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      phoneType_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional string phoneType = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearPhoneType() {
      bitField0_ = (bitField0_ & ~0x00000002);
      phoneType_ = getDefaultInstance().getPhoneType();
      onChanged();
      return this;
    }
    /**
     * <code>optional string phoneType = 2;</code>
     * @param value The bytes for phoneType to set.
     * @return This builder for chaining.
     */
    public Builder setPhoneTypeBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
      phoneType_ = value;
      onChanged();
      return this;
    }
    @java.lang.Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @java.lang.Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:com.xu.netty.seventhexample.multimsg.proto.App)
  }

  // @@protoc_insertion_point(class_scope:com.xu.netty.seventhexample.multimsg.proto.App)
  private static final com.xu.netty.seventhexample.multimsg.proto.App DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.xu.netty.seventhexample.multimsg.proto.App();
  }

  public static com.xu.netty.seventhexample.multimsg.proto.App getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  @java.lang.Deprecated public static final com.google.protobuf.Parser<App>
      PARSER = new com.google.protobuf.AbstractParser<App>() {
    @java.lang.Override
    public App parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new App(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<App> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<App> getParserForType() {
    return PARSER;
  }

  @java.lang.Override
  public com.xu.netty.seventhexample.multimsg.proto.App getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
