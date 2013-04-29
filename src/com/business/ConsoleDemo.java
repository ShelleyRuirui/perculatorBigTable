package com.business;

import java.util.ArrayList;

import com.bigTableEmu.TableManager;

public class ConsoleDemo {

	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
//		consoleConcurrentUpdate();
		consoleConcurrentThenRead();
	}
	
	public static void consoleConcurrentUpdate() throws InterruptedException{
		DocumentLauncher.prepareBigTable();
		for(int i=1;i<=6;i++){
			Document doc=new Document("http://url"+i,"abc");
			startDocConcurrent(doc);
		}
		Thread.sleep(2000);
		TableManager.getTable("dups").print();
		TableManager.getTable("document").print();
	}
	
	public static void consoleConcurrentThenRead() throws InterruptedException{
		DocumentLauncher.prepareBigTable();
		ArrayList<Document> docs=new ArrayList<Document>();
		for(int i=1;i<=6;i++){
			Document doc=new Document("http://url"+i,"abc");
			docs.add(doc);
			startDocConcurrent(doc);
		}
		Thread.sleep(2000);
		TableManager.getTable("dups").print();
		TableManager.getTable("document").print();
		System.out.println();
		System.out.println();
		System.out.println();
		DocumentLauncher l1=new DocumentLauncher(docs.get(2));
		String result1=l1.justRead();
		System.out.println("READ RESULT:http://url3  "+result1);
		DocumentLauncher l2=new DocumentLauncher(docs.get(3));
		String result2=l2.justRead();
		System.out.println("READ RESULT:http://url4  "+result2);
		
		TableManager.getTable("dups").print();
		TableManager.getTable("document").print();
	}
	
	private static void startDocConcurrent(Document doc){
		DocumentLauncher l = new DocumentLauncher(doc);
		Thread t=new Thread(l);
		t.start();
	}

}
