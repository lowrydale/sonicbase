package com.lowryengineering.research.socket;

import java.nio.channels.SocketChannel;

public class ChangeRequest {
	public static final int REGISTER = 1;
	public static final int CHANGEOPS = 2;

	private SocketChannel socket;
	private  int type;
	private  int ops;

	public ChangeRequest(SocketChannel socket, int type, int ops) {
		this.socket = socket;
		this.type = type;
		this.ops = ops;
	}

	public SocketChannel getSocket() {
		return socket;
	}

	public void setSocket(SocketChannel socket) {
		this.socket = socket;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getOps() {
		return ops;
	}

	public void setOps(int ops) {
		this.ops = ops;
	}
}
