package com.pyp.comm.publisher;

public interface Publisher {

	public void emit() throws Exception;
	public void emit(String msg) throws Exception;
}
