package com.flink.model;

import java.io.Serializable;

import lombok.Data;

@SuppressWarnings("serial")
@Data
public class CallKafkaRecord implements Serializable
{
	public int areaCode;
	public String fromPhoneNumber;
	public String toPhoneNumber;
	public int count;
	Long timestamp;

	@Override
	public String toString()
	{
		return areaCode+":"+count;
	}

}
