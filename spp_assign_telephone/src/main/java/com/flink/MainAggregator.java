package com.flink;

import com.flink.model.CareTicketRecord;

public class MainAggregator {
	
	static String BOOTSTRAP_SERVER = "localhost:9092";
	
	
	public static void main(String[] args)  throws Exception{
		CallProcessor callProcessor=new CallProcessor(BOOTSTRAP_SERVER);
		callProcessor.callCount();
		
		CareTicketProcessor careTicketProcessor=new CareTicketProcessor(BOOTSTRAP_SERVER);
		careTicketProcessor.ticketCount();
	}

	
}
