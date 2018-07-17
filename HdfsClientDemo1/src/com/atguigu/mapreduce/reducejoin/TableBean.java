package com.atguigu.mapreduce.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TableBean implements Writable {

	private String order_id;
	private String pId;
	private int amount;
	private String pName;
	private String flag;

	public TableBean() {
		super();
	}

	public TableBean(String order_id, String pId, int amount, String pName, String flag) {
		super();
		this.order_id = order_id;
		this.pId = pId;
		this.amount = amount;
		this.pName = pName;
		this.flag = flag;
	}

	@Override
	public String toString() {
		return order_id + "\t" + pName + "\t" + amount + "\t";
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(order_id);
		out.writeUTF(pId);
		out.writeInt(amount);
		out.writeUTF(pName);
		out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		order_id = in.readUTF();
		pId = in.readUTF();
		amount = in.readInt();
		pName = in.readUTF();
		flag = in.readUTF();
	}

	public String getOrder_id() {
		return order_id;
	}

	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}

	public String getpId() {
		return pId;
	}

	public void setpId(String pId) {
		this.pId = pId;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getpName() {
		return pName;
	}

	public void setpName(String pName) {
		this.pName = pName;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

}
