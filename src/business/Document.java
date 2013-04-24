package business;

public class Document {

	String url;
	String contents;
	public Document(String url, String contents) {
		this.url = url;
		this.contents = contents;
	}
	
	public String toString(){
		return "URL:"+url+"   Contents:"+contents;
	}
}
