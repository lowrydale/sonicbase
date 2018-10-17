/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.stream;

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