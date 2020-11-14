package com.flink.model;

import com.flink.connector.Producer;

public class CallGenerator extends Thread 
{
	int counter = 0;
	final Producer<String> p;
	final String topic;

	public CallGenerator(Producer<String> p, String topic)
	{
		this.p = p;
		this.topic = topic;
	}

	@Override
	public void run() 
	{
		try 
		{
			while( ++counter > 0 )
			{
				CallRecord mySchema=new CallRecord();
				p.send(topic, mySchema.toString());

				Thread.sleep( 1000 );
			}
		} 
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}
