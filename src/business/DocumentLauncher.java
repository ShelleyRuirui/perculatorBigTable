package business;

import perculator.MultilineTransaction;
import bigTableEmu.BigTable;
import bigTableEmu.Column;
import bigTableEmu.Row;
import bigTableEmu.TableManager;

public class DocumentLauncher implements Runnable {

	Document doc;

	public DocumentLauncher(Document doc) {
		this.doc = doc;
	}

	public void run() {
		updateDocument();
	}

	public boolean updateDocument() {
		MultilineTransaction tr = new MultilineTransaction();
		tr.set(new Row(doc.url), new Column("contents"), "document",
				doc.contents);
		int hash = hash(doc.contents);

		String canonical = tr.get(new Row(hash + ""), new Column(
				"canonical-url"), "dups");
		boolean updateFlag=false;
		if (canonical == null){
			tr.set(new Row(hash + ""), new Column("canonical-url"), "dups",
					doc.url);
			updateFlag=true;
		}
			

		boolean result=tr.commit();
		System.out.println(doc);
		System.out.println("UpdateFlag:"+updateFlag);
		System.out.println("UpdateResult:"+result);
		System.out.println("***********");
		return result;
	}
 
	private static int hash(String key) {
		int hash = 0;
		int i;
		for (i = 0; i < key.length(); ++i)
			hash = 33 * hash + key.charAt(i);
		return hash;
	}
	
	public static void prepareBigTable(){
		BigTable document=new BigTable();
		BigTable dups=new BigTable();
		TableManager.setTable("document", document);
		TableManager.setTable("dups", dups);
	}

	public static void main(String[] args) {
		prepareBigTable();
		test2();
		
//		consoleRun();
//		consoleRunWithClean();
	}
	
	public static void consoleRun(){
		prepareBigTable();
		
		System.out.println("Transaction 1 started");
		Document doc1=new Document("http://url1","abc");
		MultilineTransaction tr1 = new MultilineTransaction();
		tr1.set(new Row(doc1.url), new Column("contents"), "document",doc1.contents);
		System.out.println("Transaction 1 write 'document' table buffered");
		
		int hash = hash(doc1.contents);
		String canonical1 = tr1.get(new Row(hash + ""), new Column("canonical-url"), "dups");
		boolean updateFlag1=false;
		if (canonical1 == null){
			tr1.set(new Row(hash + ""), new Column("canonical-url"), "dups",doc1.url);
			updateFlag1=true;
			System.out.println("Transaction 1 write 'dups' table buffered");
		}
		
//		System.out.println(tr1.checkHaveWrites());
//		System.out.println(tr1.preWritePrimary());
//		System.out.println(tr1.prewriteSecondary());
		
		System.out.println("Transaction 2 started");
		Document doc2=new Document("http://url2","abc");
		MultilineTransaction tr2 = new MultilineTransaction();
		tr2.set(new Row(doc2.url), new Column("contents"), "document",doc2.contents);
		System.out.println("Transaction 2 write 'document' table buffered");
		
		int hash2 = hash(doc2.contents);
		String canonical2 = tr2.get(new Row(hash2 + ""), new Column("canonical-url"), "dups");
		printAllTables();
		boolean updateFlag2=false;
		if (canonical2 == null){
			tr2.set(new Row(hash2 + ""), new Column("canonical-url"), "dups",
					doc2.url);
			updateFlag2=true;
			System.out.println("Transaction 2 write 'dups' table buffered");
		}
		
		System.out.println(tr1.checkHaveWrites());
		System.out.println(tr2.checkHaveWrites());
		System.out.println(tr1.preWritePrimary());
		System.out.println(tr2.preWritePrimary());
		System.out.println(tr1.prewriteSecondary());
		System.out.println(tr2.prewriteSecondary());
		System.out.println(tr1.commitPrimary());
		tr1.commitSecondary();
	}
	
	public static void consoleRunWithClean(){
		prepareBigTable();
		System.out.println("Transaction 1 started");
		Document doc1=new Document("http://url1","abc");
		MultilineTransaction tr1 = new MultilineTransaction();
		tr1.set(new Row(doc1.url), new Column("contents"), "document",
				doc1.contents);
		System.out.println("Transaction 1 write 'document' table buffered");
		
		int hash = hash(doc1.contents);

		//The get method is split into these three steps
		tr1.beforeClean(new Row(hash + ""), new Column("canonical-url"), "dups");
		tr1.checkIfNeedClean(new Row(hash + ""), new Column("canonical-url"), "dups");
		String canonical1 = tr1.afterClean(new Row(hash + ""), new Column("canonical-url"), "dups");
		boolean updateFlag1=false;
		if (canonical1 == null){
			tr1.set(new Row(hash + ""), new Column("canonical-url"), "dups",doc1.url);
			updateFlag1=true;
			System.out.println("Transaction 1 write 'dups' table buffered");
		}
		
		System.out.println(tr1.checkHaveWrites());
		System.out.println(tr1.preWritePrimary());
		System.out.println(tr1.prewriteSecondary());
		//If below is commented, then the transaction fails. Otherwise, it is successful 
		System.out.println(tr1.commitPrimary());
		System.out.println("////////////////////////After tr1 prewrite/////////////////////");
		TableManager.getTable("dups").print();
		TableManager.getTable("document").print();
		
		//Emulate abort here after commit
		System.out.println("Transaction 2 started");
		Document doc2=new Document("http://url2","abc");
		MultilineTransaction tr2 = new MultilineTransaction();
		tr2.set(new Row(doc2.url), new Column("contents"), "document",doc2.contents);
		System.out.println("Transaction 2 write 'document' table buffered");
		
		int hash2 = hash(doc2.contents);

		//The get method is split into the three parts including the clean up
		tr2.beforeClean(new Row(hash2 + ""), new Column("canonical-url"), "dups");
		tr2.checkIfNeedClean(new Row(hash2 + ""), new Column("canonical-url"), "dups");
		String canonical2 = tr2.afterClean(new Row(hash2 + ""), new Column("canonical-url"), "dups");
		boolean updateFlag2=false;
		if (canonical2 == null){
			tr2.set(new Row(hash2 + ""), new Column("canonical-url"), "dups",
					doc2.url);
			updateFlag2=true;
			System.out.println("Transaction 2 write 'dups' table buffered");
		}
		
		TableManager.getTable("dups").print();
		TableManager.getTable("document").print();
		System.out.println("///////////////After tr2 clean////////////////");
		System.out.println(tr2.checkHaveWrites());
		System.out.println(tr2.preWritePrimary());
		System.out.println(tr2.prewriteSecondary());
		System.out.println(tr2.commitPrimary());
		tr2.commitSecondary();
		
		TableManager.getTable("dups").print();
		TableManager.getTable("document").print();
	}
	
	public static void printAllTables(){
		System.out.println("//////////////////");
		TableManager.getTable("dups").print();
		System.out.println("-----------------------");
		TableManager.getTable("document").print();
		System.out.println("//////////////////");
	}
	
	public static void test1(){
		Document doc1 = new Document("http://url1.com", "abc");
		Document doc2 = new Document("http://url2.com", "abc");
		DocumentLauncher l = new DocumentLauncher(doc1);
		DocumentLauncher l1 = new DocumentLauncher(doc2);
		Thread t1 = new Thread(l1);
		t1.start();
		l.updateDocument();
		
		TableManager.getTable("dups").print();
	}
	
	public static void test2(){
		Document doc1 = new Document("http://url1.com", "abc");
		Document doc2 = new Document("http://url2.com", "abc");
		int hash = hash(doc1.contents);
		
		MultilineTransaction tr1 = new MultilineTransaction();		
		tr1.set(new Row(doc1.url), new Column("contents"), "document",doc1.contents);
		String canonical1 = tr1.get(new Row(hash + ""), new Column("canonical-url"), "dups");
		boolean updateFlag1=false;
		
		
		
		MultilineTransaction tr2 = new MultilineTransaction();
		tr2.set(new Row(doc2.url), new Column("contents"), "document",doc2.contents);
		String canonical2 = tr2.get(new Row(hash + ""), new Column("canonical-url"), "dups");
		boolean updateFlag2=false;
		if (canonical2 == null){
			tr2.set(new Row(hash + ""), new Column("canonical-url"), "dups",doc2.url);
			updateFlag2=true;
		}
		
		if (canonical1 == null){
			tr1.set(new Row(hash + ""), new Column("canonical-url"), "dups",doc1.url);
			updateFlag1=true;
		}
		boolean result1=tr1.commit();	
		
		boolean result2=tr2.commit();
		
		System.out.println("UpdateFlag1:"+updateFlag1);
		System.out.println("CommitFlag1:"+result1);
		System.out.println("UpdateFlag2:"+updateFlag2);
		System.out.println("CommitFlag2:"+result2);
		TableManager.getTable("dups").print();
		TableManager.getTable("document").print();
	}
}
