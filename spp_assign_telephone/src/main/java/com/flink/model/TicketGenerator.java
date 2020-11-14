package com.flink.model;

import com.flink.connector.Producer;

public class TicketGenerator extends Thread 
{
	int counter = 0;
	final Producer<String> p;
	final String topic;

	public TicketGenerator(Producer<String> p, String topic)
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
				CareTicketRecord mySchema=new CareTicketRecord();
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
