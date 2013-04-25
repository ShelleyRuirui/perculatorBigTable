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

	private int hash(String key) {
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
		
		Document doc1 = new Document("http://url1.com", "abc");
		Document doc2 = new Document("http://url2.com", "abc");
		DocumentLauncher l = new DocumentLauncher(doc1);
		DocumentLauncher l1 = new DocumentLauncher(doc2);
		Thread t1 = new Thread(l1);
		t1.start();
		l.updateDocument();
		
		TableManager.getTable("dups").print();
	}
}
