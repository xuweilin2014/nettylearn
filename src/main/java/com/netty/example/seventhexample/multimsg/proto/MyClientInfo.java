// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: src/protobuf/message.proto

package com.netty.example.seventhexample.multimsg.proto;

public final class MyClientInfo {
  private MyClientInfo() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_com_xu_netty_seventhexample_multimsg_proto_MyMessage_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_com_xu_netty_seventhexample_multimsg_proto_MyMessage_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_com_xu_netty_seventhexample_multimsg_proto_App_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_com_xu_netty_seventhexample_multimsg_proto_App_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_com_xu_netty_seventhexample_multimsg_proto_Pad_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_com_xu_netty_seventhexample_multimsg_proto_Pad_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_com_xu_netty_seventhexample_multimsg_proto_Computer_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_com_xu_netty_seventhexample_multimsg_proto_Computer_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\032src/protobuf/message.proto\022*com.xu.net" +
      "ty.seventhexample.multimsg.proto\"\335\002\n\tMyM" +
      "essage\022O\n\004type\030\001 \002(\0162A.com.xu.netty.seve" +
      "nthexample.multimsg.proto.MyMessage.Mess" +
      "ageType\022>\n\003app\030\002 \001(\0132/.com.xu.netty.seve" +
      "nthexample.multimsg.proto.AppH\000\022>\n\003pad\030\003" +
      " \001(\0132/.com.xu.netty.seventhexample.multi" +
      "msg.proto.PadH\000\022H\n\010computer\030\004 \001(\01324.com." +
      "xu.netty.seventhexample.multimsg.proto.C" +
      "omputerH\000\"-\n\013MessageType\022\007\n\003App\020\001\022\007\n\003Pad" +
      "\020\002\022\014\n\010Computer\020\003B\006\n\004body\"$\n\003App\022\n\n\002ip\030\001 " +
      "\001(\t\022\021\n\tphoneType\030\002 \001(\t\"\"\n\003Pad\022\n\n\002ip\030\001 \001(" +
      "\t\022\017\n\007padType\030\002 \001(\t\",\n\010Computer\022\n\n\002ip\030\001 \001" +
      "(\t\022\024\n\014computerType\030\002 \001(\tB>\n*com.xu.netty" +
      ".seventhexample.multimsg.protoB\014MyClient" +
      "InfoH\001P\001"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_com_xu_netty_seventhexample_multimsg_proto_MyMessage_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_com_xu_netty_seventhexample_multimsg_proto_MyMessage_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_com_xu_netty_seventhexample_multimsg_proto_MyMessage_descriptor,
        new java.lang.String[] { "Type", "App", "Pad", "Computer", "Body", });
    internal_static_com_xu_netty_seventhexample_multimsg_proto_App_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_com_xu_netty_seventhexample_multimsg_proto_App_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_com_xu_netty_seventhexample_multimsg_proto_App_descriptor,
        new java.lang.String[] { "Ip", "PhoneType", });
    internal_static_com_xu_netty_seventhexample_multimsg_proto_Pad_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_com_xu_netty_seventhexample_multimsg_proto_Pad_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_com_xu_netty_seventhexample_multimsg_proto_Pad_descriptor,
        new java.lang.String[] { "Ip", "PadType", });
    internal_static_com_xu_netty_seventhexample_multimsg_proto_Computer_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_com_xu_netty_seventhexample_multimsg_proto_Computer_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_com_xu_netty_seventhexample_multimsg_proto_Computer_descriptor,
        new java.lang.String[] { "Ip", "ComputerType", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}