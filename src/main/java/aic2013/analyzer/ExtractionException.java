package aic2013.analyzer;

/**
 * @author Moritz Becker (moritz.becker@gmx.at)
 *
 */
public class ExtractionException extends Exception {
	public ExtractionException() {
		// TODO Auto-generated constructor stub
	}
	
	public ExtractionException(String msg){
		super(msg);
	}
	
	public ExtractionException(Throwable t){
		super(t);
	}
	
	public ExtractionException(String msg, Throwable t){
		super(msg, t);
	}
}
