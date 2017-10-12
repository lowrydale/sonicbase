package com.sonicbase.queue;


public class Message {
  private String body;

  public Message() { }

  public Message(String body) {
    this.body = body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public String getBody() {
    return body;
  }
}