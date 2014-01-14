/**
 * 
 */
package aic2013.analyzer.filter;

/**
 * @author Moritz Becker (moritz.becker@gmx.at)
 *
 */
public class GlobalPrefixFilter extends TextFilterDecorator {

	private String prefix;
	private boolean caseSensitive;
	/**
	 * @param decoratedFilter
	 */
	public GlobalPrefixFilter(String prefix, boolean caseSensitive, TextFilter decoratedFilter) {
		super(decoratedFilter);
		this.prefix = prefix;
		this.caseSensitive = caseSensitive;
	}

	 
	/* (non-Javadoc)
	 * @see aic2013.extractor.TextFilterDecorator#filter(java.lang.String)
	 */
	@Override
	public String filter(String input) {
		boolean removePrefix = false;
		input = input.trim();
		if(caseSensitive){
			if(input.startsWith(prefix)){
				removePrefix = true;
			}
		}else{
			if(input.toLowerCase().startsWith(prefix.toLowerCase())){
				removePrefix = true;
			}
		}
		if(removePrefix){
			input = input.substring(prefix.length(), input.length());
		}
		return decoratedFilter.filter(input);
	}
}
