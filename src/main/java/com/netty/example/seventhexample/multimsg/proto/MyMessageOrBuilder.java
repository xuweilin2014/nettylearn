// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: src/protobuf/message.proto

package com.netty.example.seventhexample.multimsg.proto;

public interface MyMessageOrBuilder extends
    // @@protoc_insertion_point(interface_extends:com.xu.netty.seventhexample.multimsg.proto.MyMessage)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>required .com.xu.netty.seventhexample.multimsg.proto.MyMessage.MessageType type = 1;</code>
   * @return Whether the type field is set.
   */
  boolean hasType();
  /**
   * <code>required .com.xu.netty.seventhexample.multimsg.proto.MyMessage.MessageType type = 1;</code>
   * @return The type.
   */
  com.netty.example.seventhexample.multimsg.proto.MyMessage.MessageType getType();

  /**
   * <code>optional .com.xu.netty.seventhexample.multimsg.proto.App app = 2;</code>
   * @return Whether the app field is set.
   */
  boolean hasApp();
  /**
   * <code>optional .com.xu.netty.seventhexample.multimsg.proto.App app = 2;</code>
   * @return The app.
   */
  com.netty.example.seventhexample.multimsg.proto.App getApp();
  /**
   * <code>optional .com.xu.netty.seventhexample.multimsg.proto.App app = 2;</code>
   */
  com.netty.example.seventhexample.multimsg.proto.AppOrBuilder getAppOrBuilder();

  /**
   * <code>optional .com.xu.netty.seventhexample.multimsg.proto.Pad pad = 3;</code>
   * @return Whether the pad field is set.
   */
  boolean hasPad();
  /**
   * <code>optional .com.xu.netty.seventhexample.multimsg.proto.Pad pad = 3;</code>
   * @return The pad.
   */
  com.netty.example.seventhexample.multimsg.proto.Pad getPad();
  /**
   * <code>optional .com.xu.netty.seventhexample.multimsg.proto.Pad pad = 3;</code>
   */
  com.netty.example.seventhexample.multimsg.proto.PadOrBuilder getPadOrBuilder();

  /**
   * <code>optional .com.xu.netty.seventhexample.multimsg.proto.Computer computer = 4;</code>
   * @return Whether the computer field is set.
   */
  boolean hasComputer();
  /**
   * <code>optional .com.xu.netty.seventhexample.multimsg.proto.Computer computer = 4;</code>
   * @return The computer.
   */
  com.netty.example.seventhexample.multimsg.proto.Computer getComputer();
  /**
   * <code>optional .com.xu.netty.seventhexample.multimsg.proto.Computer computer = 4;</code>
   */
  com.netty.example.seventhexample.multimsg.proto.ComputerOrBuilder getComputerOrBuilder();

  public com.netty.example.seventhexample.multimsg.proto.MyMessage.BodyCase getBodyCase();
}
