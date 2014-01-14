/**
 * 
 */
package aic2013.analyzer.filter;

/**
 * @author Moritz Becker (moritz.becker@gmx.at)
 *
 */
public class BaseFilter implements TextFilter {

	/* (non-Javadoc)
	 * @see aic2013.analyzer.filter.TextFilter#filter(java.lang.String)
	 */
	public String filter(String input) {
		return input;
	}
}
