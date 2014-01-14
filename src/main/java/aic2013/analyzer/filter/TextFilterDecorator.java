/**
 * 
 */
package aic2013.analyzer.filter;

/**
 * @author Moritz Becker (moritz.becker@gmx.at)
 *
 */
public abstract class TextFilterDecorator implements TextFilter{
	protected TextFilter decoratedFilter;
	
	public TextFilterDecorator(TextFilter decoratedFilter){
		this.decoratedFilter = decoratedFilter;
	}
	
	public String filter(String input) {
		return decoratedFilter.filter(input);
	}
}
